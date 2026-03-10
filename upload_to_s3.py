#!/usr/bin/env python3
"""Upload DailyWord data to S3 and configure CloudFront."""

import argparse
import json
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

from config import FINAL_DATA_DIR

BUCKET_NAME = "dailyword-words-v2"
REGION = "ap-southeast-1"
S3_PREFIX = "words/"
CLOUDFRONT_DISTRIBUTION_ID = "E34S0D0HGF4Q4B"
OAC_NAME = "dailyword-words-v2-oac"


def get_s3_client():
    return boto3.client("s3", region_name=REGION)


def get_cloudfront_client():
    return boto3.client("cloudfront")


# ── init-bucket ──────────────────────────────────────────────


def init_bucket():
    """Create S3 bucket, block public access, create OAC, and set bucket policy."""
    s3 = get_s3_client()
    cf = get_cloudfront_client()

    # 1. Create bucket
    try:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"Created bucket: {BUCKET_NAME}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Bucket already exists: {BUCKET_NAME}")
        else:
            raise

    # 2. Block all public access
    s3.put_public_access_block(
        Bucket=BUCKET_NAME,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print("Public access blocked.")

    # 3. Create OAC
    oac_id = _create_or_get_oac(cf)
    print(f"OAC ready: {oac_id}")

    # 4. Set bucket policy for CloudFront OAC
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCloudFrontServicePrincipalReadOnly",
                "Effect": "Allow",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*",
                "Condition": {
                    "StringEquals": {
                        "AWS:SourceArn": f"arn:aws:cloudfront::{_get_account_id()}:distribution/{CLOUDFRONT_DISTRIBUTION_ID}"
                    }
                },
            }
        ],
    }
    s3.put_bucket_policy(Bucket=BUCKET_NAME, Policy=json.dumps(policy))
    print("Bucket policy set for CloudFront OAC access.")
    print("\nDone! Next steps:")
    print("  1. Upload words:  python upload_to_s3.py")
    print("  2. Update CDN:    python upload_to_s3.py --update-cloudfront")


def _get_account_id():
    sts = boto3.client("sts")
    return sts.get_caller_identity()["Account"]


def _create_or_get_oac(cf):
    """Create OAC or return existing one by name."""
    # Check if OAC already exists
    paginator = cf.get_paginator("list_origin_access_controls")
    for page in paginator.paginate():
        for item in page["OriginAccessControlList"].get("Items", []):
            if item["Name"] == OAC_NAME:
                return item["Id"]

    # Create new OAC
    resp = cf.create_origin_access_control(
        OriginAccessControlConfig={
            "Name": OAC_NAME,
            "Description": "OAC for DailyWord words S3 bucket",
            "SigningProtocol": "sigv4",
            "SigningBehavior": "always",
            "OriginAccessControlOriginType": "s3",
        }
    )
    return resp["OriginAccessControl"]["Id"]


# ── upload ───────────────────────────────────────────────────


def discover_words(words=None):
    """Discover word folders and their latest JSON files.

    Returns list of (word_folder_name, json_path) tuples.
    """
    if not FINAL_DATA_DIR.is_dir():
        print(f"Error: {FINAL_DATA_DIR} does not exist.")
        sys.exit(1)

    if words:
        folders = []
        for w in words:
            # Try exact folder name first, then sanitized
            folder = FINAL_DATA_DIR / w
            if not folder.is_dir():
                print(f"Warning: folder not found for word '{w}', skipping.")
                continue
            folders.append(folder)
    else:
        folders = sorted(
            [d for d in FINAL_DATA_DIR.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )

    results = []
    for folder in folders:
        json_files = sorted(folder.glob("*.json"))
        if not json_files:
            print(f"Warning: no JSON file in {folder.name}/, skipping.")
            continue
        # Pick the most recent by filename sort (timestamp in name)
        latest = json_files[-1]
        results.append((folder.name, latest))

    return results


def upload_words(words=None, dry_run=False):
    """Upload word JSON files to S3."""
    entries = discover_words(words)

    if not entries:
        print("No words to upload.")
        return

    if dry_run:
        print(f"Dry run — {len(entries)} words would be uploaded:\n")
        for folder_name, json_path in entries:
            s3_key = f"{S3_PREFIX}{folder_name}.json"
            print(f"  {json_path.relative_to(FINAL_DATA_DIR.parent)} → {s3_key}")
        return

    s3 = get_s3_client()
    uploaded = 0
    failed = 0

    for folder_name, json_path in tqdm(entries, desc="Uploading"):
        s3_key = f"{S3_PREFIX}{folder_name}.json"
        try:
            s3.upload_file(
                str(json_path),
                BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    "ContentType": "application/json",
                    "CacheControl": "public, max-age=86400",
                },
            )
            uploaded += 1
        except ClientError as e:
            print(f"\nFailed to upload {folder_name}: {e}")
            failed += 1

    print(f"\nUploaded: {uploaded}, Failed: {failed}, Total: {len(entries)}")


# ── update-cloudfront ────────────────────────────────────────


def update_cloudfront():
    """Add or update the S3 origin and words/* cache behavior on the CloudFront distribution."""
    cf = get_cloudfront_client()

    # Get current config
    resp = cf.get_distribution_config(Id=CLOUDFRONT_DISTRIBUTION_ID)
    config = resp["DistributionConfig"]
    etag = resp["ETag"]

    # Find OAC ID
    oac_id = None
    paginator = cf.get_paginator("list_origin_access_controls")
    for page in paginator.paginate():
        for item in page["OriginAccessControlList"].get("Items", []):
            if item["Name"] == OAC_NAME:
                oac_id = item["Id"]
                break
        if oac_id:
            break

    if not oac_id:
        print("Error: OAC not found. Run --init-bucket first.")
        sys.exit(1)

    origin_id = "S3-dailyword-words-v2"
    origin_domain = f"{BUCKET_NAME}.s3.{REGION}.amazonaws.com"

    # ── Upsert origin ──
    new_origin = {
        "Id": origin_id,
        "DomainName": origin_domain,
        "OriginPath": "",
        "CustomHeaders": {"Quantity": 0},
        "S3OriginConfig": {"OriginAccessIdentity": ""},
        "OriginAccessControlId": oac_id,
        "OriginShield": {"Enabled": False},
        "ConnectionAttempts": 3,
        "ConnectionTimeout": 10,
    }

    origins = config["Origins"]["Items"]
    replaced = False
    for i, o in enumerate(origins):
        if o["Id"] == origin_id:
            origins[i] = new_origin
            replaced = True
            break
    if not replaced:
        origins.append(new_origin)
        config["Origins"]["Quantity"] = len(origins)

    # ── Upsert cache behavior for words/* ──
    new_behavior = {
        "PathPattern": "words/*",
        "TargetOriginId": origin_id,
        "ViewerProtocolPolicy": "redirect-to-https",
        "AllowedMethods": {
            "Quantity": 2,
            "Items": ["GET", "HEAD"],
            "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]},
        },
        "Compress": True,
        "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",  # CachingOptimized
        "SmoothStreaming": False,
        "FieldLevelEncryptionId": "",
        "LambdaFunctionAssociations": {"Quantity": 0},
        "FunctionAssociations": {"Quantity": 0},
    }

    behaviors = config.get("CacheBehaviors", {}).get("Items", [])
    replaced = False
    for i, b in enumerate(behaviors):
        if b["PathPattern"] == "words/*":
            behaviors[i] = new_behavior
            replaced = True
            break
    if not replaced:
        behaviors.append(new_behavior)

    config["CacheBehaviors"] = {
        "Quantity": len(behaviors),
        "Items": behaviors,
    }

    # Show summary and confirm
    print("CloudFront distribution update summary:")
    print(f"  Distribution: {CLOUDFRONT_DISTRIBUTION_ID}")
    print(f"  Origin: {origin_domain} (OAC: {oac_id})")
    print(f"  Cache behavior: words/* → {origin_id}")
    print(f"  {'Replacing' if replaced else 'Adding'} existing words/* behavior")
    answer = input("\nApply this update? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    cf.update_distribution(
        Id=CLOUDFRONT_DISTRIBUTION_ID,
        DistributionConfig=config,
        IfMatch=etag,
    )
    print("CloudFront distribution updated successfully.")
    print("Note: changes may take a few minutes to propagate.")


# ── CLI ──────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Upload DailyWord data to S3 and configure CloudFront",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_to_s3.py --init-bucket              # One-time: create S3 bucket
  python upload_to_s3.py                             # Upload all words
  python upload_to_s3.py --words abandon,ability     # Upload specific words
  python upload_to_s3.py --update-cloudfront          # Update CloudFront config
  python upload_to_s3.py --dry-run                   # Preview uploads
        """,
    )
    parser.add_argument(
        "--init-bucket",
        action="store_true",
        help="Create S3 bucket with CloudFront OAC access policy",
    )
    parser.add_argument(
        "--update-cloudfront",
        action="store_true",
        help="Add/update S3 origin and words/* cache behavior on CloudFront",
    )
    parser.add_argument(
        "--words", "-w",
        type=str,
        default=None,
        help="Comma-separated list of words to upload (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be uploaded without uploading",
    )

    args = parser.parse_args()

    if args.init_bucket:
        init_bucket()
    elif args.update_cloudfront:
        update_cloudfront()
    else:
        word_list = [w.strip() for w in args.words.split(",")] if args.words else None
        upload_words(words=word_list, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
