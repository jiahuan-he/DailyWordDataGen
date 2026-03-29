#!/usr/bin/env python3
"""Upload DailyWord data to S3 and configure CloudFront."""

import argparse
import json
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

import config as app_config

BUCKET_NAME = "dailyword-words-v2"
SOURCE_DIR = Path(__file__).parent / "source"
REGION = "ap-southeast-1"
S3_PREFIX = "words/"
S3_AUDIO_PREFIX = "audio/"
CLOUDFRONT_DISTRIBUTION_ID = "E34S0D0HGF4Q4B"
OAC_NAME = "dailyword-words-v2-oac"


def get_s3_client():
    return boto3.client("s3", region_name=REGION)


def get_cloudfront_client():
    return boto3.client("cloudfront")


def list_s3_words(s3=None):
    """List all word names already in S3 under the words/ prefix."""
    if s3 is None:
        s3 = get_s3_client()
    s3_words = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key[len(S3_PREFIX):]
            if name.endswith(".json"):
                s3_words.add(name[:-5])
    return s3_words


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
    if not app_config.FINAL_DATA_DIR.is_dir():
        print(f"Error: {app_config.FINAL_DATA_DIR} does not exist.")
        sys.exit(1)

    if words:
        folders = []
        for w in words:
            # Try exact folder name first, then sanitized
            folder = app_config.FINAL_DATA_DIR / w
            if not folder.is_dir():
                print(f"Warning: folder not found for word '{w}', skipping.")
                continue
            folders.append(folder)
    else:
        folders = sorted(
            [d for d in app_config.FINAL_DATA_DIR.iterdir() if d.is_dir()],
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
            print(f"  {json_path.relative_to(app_config.FINAL_DATA_DIR.parent)} → {s3_key}")
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


def upload_words_incremental(words=None, dry_run=False, force=False):
    """Upload only new word JSON files to S3 (words not yet in S3).

    With --force, uploads all words regardless.
    """
    if force:
        upload_words(words=words, dry_run=dry_run)
        return

    entries = discover_words(words)
    if not entries:
        print("No words to upload.")
        return

    print("Checking S3 for existing words...")
    s3 = get_s3_client()
    s3_words = list_s3_words(s3)
    print(f"Found {len(s3_words)} words in S3.")

    to_upload = [(name, path) for name, path in entries if name not in s3_words]
    skipped = len(entries) - len(to_upload)

    if not to_upload:
        print(f"All {len(entries)} words already in S3. Nothing to upload.")
        return

    print(f"{len(to_upload)} new words to upload, {skipped} already in S3.")

    if dry_run:
        print(f"\nDry run — new words that would be uploaded:\n")
        for folder_name, json_path in to_upload:
            s3_key = f"{S3_PREFIX}{folder_name}.json"
            print(f"  {json_path.relative_to(app_config.FINAL_DATA_DIR.parent)} → {s3_key}")
        return

    uploaded = 0
    failed = 0
    for folder_name, json_path in tqdm(to_upload, desc="Uploading"):
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

    print(f"\nUploaded: {uploaded}, Failed: {failed}, Skipped: {skipped}")


# ── update-cloudfront ────────────────────────────────────────


def update_cloudfront():
    """Add or update the S3 origin and words/* cache behavior on the CloudFront distribution."""
    cf = get_cloudfront_client()

    # Get current distribution config
    resp = cf.get_distribution_config(Id=CLOUDFRONT_DISTRIBUTION_ID)
    dist_config = resp["DistributionConfig"]
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

    origins = dist_config["Origins"]["Items"]
    replaced = False
    for i, o in enumerate(origins):
        if o["Id"] == origin_id:
            origins[i] = new_origin
            replaced = True
            break
    if not replaced:
        origins.append(new_origin)
        dist_config["Origins"]["Quantity"] = len(origins)

    # ── Upsert cache behaviors for words/* and audio/* ──
    cache_patterns = ["words/*", "audio/*"]
    behaviors = dist_config.get("CacheBehaviors", {}).get("Items", [])

    base_behavior = {
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

    for pattern in cache_patterns:
        new_behavior = {**base_behavior, "PathPattern": pattern}
        replaced = False
        for i, b in enumerate(behaviors):
            if b["PathPattern"] == pattern:
                behaviors[i] = new_behavior
                replaced = True
                break
        if not replaced:
            behaviors.append(new_behavior)

    dist_config["CacheBehaviors"] = {
        "Quantity": len(behaviors),
        "Items": behaviors,
    }

    # Show summary and confirm
    print("CloudFront distribution update summary:")
    print(f"  Distribution: {CLOUDFRONT_DISTRIBUTION_ID}")
    print(f"  Origin: {origin_domain} (OAC: {oac_id})")
    print(f"  Cache behaviors: {', '.join(cache_patterns)} → {origin_id}")
    answer = input("\nApply this update? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    cf.update_distribution(
        Id=CLOUDFRONT_DISTRIBUTION_ID,
        DistributionConfig=dist_config,
        IfMatch=etag,
    )
    print("CloudFront distribution updated successfully.")
    print("Note: changes may take a few minutes to propagate.")


# ── metadata ─────────────────────────────────────────────────


def upload_metadata():
    """Upload metadata files (word_order.json, word_levels.json) to S3."""
    s3 = get_s3_client()
    metadata_files = ["word_order.json", "word_levels.json"]
    uploaded = 0
    failed = 0

    for filename in metadata_files:
        filepath = SOURCE_DIR / filename
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping.")
            continue
        try:
            s3.upload_file(
                str(filepath),
                BUCKET_NAME,
                filename,
                ExtraArgs={
                    "ContentType": "application/json",
                    "CacheControl": "public, max-age=86400",
                },
            )
            print(f"Uploaded {filename}")
            uploaded += 1
        except ClientError as e:
            print(f"Failed to upload {filename}: {e}")
            failed += 1

    print(f"\nDone. Uploaded: {uploaded}, Failed: {failed}")

    # Invalidate CloudFront cache for metadata files
    if uploaded > 0:
        try:
            cf = get_cloudfront_client()
            paths = [f"/{f}" for f in metadata_files]
            resp = cf.create_invalidation(
                DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
                InvalidationBatch={
                    "Paths": {"Quantity": len(paths), "Items": paths},
                    "CallerReference": str(int(__import__("time").time())),
                },
            )
            inv_id = resp["Invalidation"]["Id"]
            print(f"CloudFront cache invalidation created: {inv_id}")
        except ClientError as e:
            print(f"Warning: CloudFront invalidation failed: {e}")


# ── wipe-and-upload ──────────────────────────────────────────


def wipe_bucket(s3):
    """Delete all objects in the bucket."""
    deleted_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_keys = [{"Key": obj["Key"]} for obj in objects]
        s3.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={"Objects": delete_keys, "Quiet": True},
        )
        deleted_count += len(delete_keys)
    return deleted_count


def wipe_and_upload():
    """Wipe all objects in the bucket and re-upload everything."""
    entries = discover_words()
    metadata_files = ["word_order.json", "word_levels.json"]

    # Summary
    print(f"Will upload {len(entries)} word JSON files + {', '.join(metadata_files)}")
    print(f"Target bucket: {BUCKET_NAME}")
    answer = input(
        "\nThis will DELETE all objects in '{}' and re-upload. Are you sure? [y/N] ".format(
            BUCKET_NAME
        )
    ).strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    s3 = get_s3_client()

    # 1. Wipe
    deleted = wipe_bucket(s3)
    print(f"Deleted {deleted} existing objects.")

    # 2. Upload word JSONs
    uploaded = 0
    failed = 0
    for folder_name, json_path in tqdm(entries, desc="Uploading words"):
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

    # 3. Upload metadata files
    for filename in metadata_files:
        filepath = SOURCE_DIR / filename
        if not filepath.exists():
            print(f"Warning: {filepath} not found, skipping.")
            continue
        try:
            s3.upload_file(
                str(filepath),
                BUCKET_NAME,
                filename,
                ExtraArgs={
                    "ContentType": "application/json",
                    "CacheControl": "public, max-age=86400",
                },
            )
            print(f"Uploaded {filename}")
        except ClientError as e:
            print(f"Failed to upload {filename}: {e}")
            failed += 1

    print(f"\nDone. Words uploaded: {uploaded}, Failed: {failed}")

    # 4. Invalidate CloudFront cache
    try:
        cf = get_cloudfront_client()
        resp = cf.create_invalidation(
            DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/*"]},
                "CallerReference": str(int(__import__("time").time())),
            },
        )
        inv_id = resp["Invalidation"]["Id"]
        print(f"CloudFront cache invalidation created: {inv_id}")
    except ClientError as e:
        print(f"Warning: CloudFront invalidation failed: {e}")


# ── audio upload ────────────────────────────────────────────


AUDIO_FILES = (
    ["word.mp3"]
    + [f"sentence_{i}.mp3" for i in range(1, app_config.EXAMPLES_PER_WORD + 1)]
    + ["metadata.json"]
)


def discover_audio_words(voice_key):
    """Discover word folders with complete audio for a voice.

    Returns list of word folder names that have all expected files.
    """
    audio_dir = app_config.AUDIO_DATA_DIR / voice_key
    if not audio_dir.is_dir():
        return []

    results = []
    for folder in sorted(audio_dir.iterdir()):
        if not folder.is_dir():
            continue
        # Check all expected files exist
        if all((folder / f).exists() for f in AUDIO_FILES):
            results.append(folder.name)
    return results


def list_s3_audio_words(voice_key, s3=None):
    """List word names that have audio in S3 for a given voice.

    Checks for presence of word.mp3 as the indicator file.
    """
    if s3 is None:
        s3 = get_s3_client()
    prefix = f"{S3_AUDIO_PREFIX}{voice_key}/"
    words = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # audio/{voice_key}/{word}/word.mp3
            parts = key[len(prefix):].split("/")
            if len(parts) == 2 and parts[1] == "word.mp3":
                words.add(parts[0])
    return words


def upload_audio(voice_key, words=None, dry_run=False, force=False):
    """Upload audio files to S3 for a voice, incrementally."""
    local_words = discover_audio_words(voice_key)
    if not local_words:
        print(f"No audio found locally for voice '{voice_key}'.")
        return

    if words:
        local_words = [w for w in local_words if w in set(words)]

    if not force:
        print(f"Checking S3 for existing audio ({voice_key})...")
        s3 = get_s3_client()
        s3_words = list_s3_audio_words(voice_key, s3)
        print(f"Found {len(s3_words)} words with audio in S3.")
        to_upload = [w for w in local_words if w not in s3_words]
        skipped = len(local_words) - len(to_upload)
    else:
        s3 = get_s3_client()
        to_upload = local_words
        skipped = 0

    if not to_upload:
        print(f"All {len(local_words)} words already in S3. Nothing to upload.")
        return

    print(f"{len(to_upload)} words to upload, {skipped} already in S3.")

    if dry_run:
        print(f"\nDry run — words that would be uploaded:")
        for word in to_upload[:20]:
            print(f"  audio/{voice_key}/{word}/ (6 files)")
        if len(to_upload) > 20:
            print(f"  ... and {len(to_upload) - 20} more")
        return

    uploaded_words = 0
    failed_words = 0
    audio_dir = app_config.AUDIO_DATA_DIR / voice_key

    for word in tqdm(to_upload, desc=f"Uploading {voice_key}"):
        word_dir = audio_dir / word
        word_failed = False
        for filename in AUDIO_FILES:
            filepath = word_dir / filename
            s3_key = f"{S3_AUDIO_PREFIX}{voice_key}/{word}/{filename}"

            content_type = "audio/mpeg" if filename.endswith(".mp3") else "application/json"
            try:
                s3.upload_file(
                    str(filepath),
                    BUCKET_NAME,
                    s3_key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "CacheControl": "public, max-age=604800",
                    },
                )
            except ClientError as e:
                print(f"\nFailed to upload {s3_key}: {e}")
                word_failed = True
                break

        if word_failed:
            failed_words += 1
        else:
            uploaded_words += 1

    print(f"\nUploaded: {uploaded_words} words ({uploaded_words * len(AUDIO_FILES)} files), "
          f"Failed: {failed_words}, Skipped: {skipped}")


def upload_voice_registry(dry_run=False):
    """Generate voice_registry.json from config and upload to S3."""
    voices = []
    for key in sorted(app_config.VOICES.keys()):
        v = app_config.VOICES[key]
        voices.append({
            "key": key,
            "accent": v["accent"],
            "gender": v["gender"],
            "style": v["style"],
        })

    registry = {"voices": voices}
    registry_json = json.dumps(registry, indent=2, ensure_ascii=False)

    if dry_run:
        print("Dry run — voice_registry.json would be uploaded:")
        print(registry_json)
        return

    s3 = get_s3_client()
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="voice_registry.json",
        Body=registry_json.encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=86400",
    )
    print("Uploaded voice_registry.json")

    # Invalidate CloudFront cache
    try:
        cf = get_cloudfront_client()
        resp = cf.create_invalidation(
            DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/voice_registry.json"]},
                "CallerReference": str(int(__import__("time").time())),
            },
        )
        print(f"CloudFront invalidation created: {resp['Invalidation']['Id']}")
    except ClientError as e:
        print(f"Warning: CloudFront invalidation failed: {e}")


# ── CLI ──────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Upload DailyWord data to S3 and configure CloudFront",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_to_s3.py --init-bucket                          # One-time: create S3 bucket
  python upload_to_s3.py                                        # Upload new words only (incremental)
  python upload_to_s3.py --force                                # Force upload all words
  python upload_to_s3.py --words abandon,ability                # Upload specific words (if not in S3)
  python upload_to_s3.py --update-cloudfront                    # Update CloudFront config
  python upload_to_s3.py --dry-run                              # Preview what would be uploaded
  python upload_to_s3.py --metadata                             # Upload metadata files only
  python upload_to_s3.py --wipe-and-upload                      # Wipe bucket and re-upload everything
  python upload_to_s3.py --audio                                # Upload new audio (incremental)
  python upload_to_s3.py --audio --voice american_woman_calm    # Upload audio for specific voice
  python upload_to_s3.py --audio --force                        # Re-upload all audio
  python upload_to_s3.py --voice-registry                       # Upload voice_registry.json
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
        help="Add/update S3 origin and cache behaviors on CloudFront",
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
    parser.add_argument(
        "--metadata",
        action="store_true",
        help="Upload metadata files (word_order.json, word_levels.json) only",
    )
    parser.add_argument(
        "--wipe-and-upload",
        action="store_true",
        help="Delete all objects in bucket and re-upload everything",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload all words/audio, even if they already exist in S3",
    )
    parser.add_argument(
        "--audio",
        action="store_true",
        help="Upload audio files instead of word JSON files",
    )
    parser.add_argument(
        "--voice",
        type=str,
        default=None,
        help="Voice key for audio upload (default: all voices with local audio)",
    )
    parser.add_argument(
        "--voice-registry",
        action="store_true",
        help="Upload voice_registry.json to S3",
    )

    args = parser.parse_args()

    if args.init_bucket:
        init_bucket()
    elif args.update_cloudfront:
        update_cloudfront()
    elif args.metadata:
        upload_metadata()
    elif args.wipe_and_upload:
        wipe_and_upload()
    elif args.voice_registry:
        upload_voice_registry(dry_run=args.dry_run)
    elif args.audio:
        word_list = [w.strip() for w in args.words.split(",")] if args.words else None
        if args.voice:
            if args.voice not in app_config.VOICES:
                print(f"Error: Unknown voice '{args.voice}'. Available: {', '.join(sorted(app_config.VOICES.keys()))}")
                sys.exit(1)
            voice_keys = [args.voice]
        else:
            voice_keys = sorted(app_config.VOICES.keys())
        for voice_key in voice_keys:
            print(f"\n{'=' * 60}")
            print(f"Audio upload: {voice_key}")
            print(f"{'=' * 60}")
            upload_audio(voice_key, words=word_list, dry_run=args.dry_run, force=args.force)
    else:
        word_list = [w.strip() for w in args.words.split(",")] if args.words else None
        upload_words_incremental(words=word_list, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
