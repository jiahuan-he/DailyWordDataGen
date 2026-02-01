#!/usr/bin/env python3
"""DailyWord Data Generation Pipeline - Main Orchestrator."""

import argparse
import sys
from datetime import datetime

import config
from src.step1_selection import run_step1
from src.step2_enrichment import run_step2
from src.step3_generation import run_step3


def parse_word_range(range_str: str) -> tuple[int, int]:
    """Parse word range string like '0-100' into tuple."""
    if "-" not in range_str:
        raise ValueError(f"Invalid range format: {range_str}. Use format: start-end")
    parts = range_str.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid range format: {range_str}. Use format: start-end")
    return int(parts[0]), int(parts[1])


def main():
    parser = argparse.ArgumentParser(
        description="DailyWord Data Generation Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline (dry run with 10 words)
  python main.py --dry-run

  # Run specific steps
  python main.py --start-step 1 --end-step 1
  python main.py --start-step 2 --end-step 2
  python main.py --start-step 3 --end-step 3

  # Process specific word range (for batching)
  python main.py --start-step 3 --word-range 0-100

  # Resume from checkpoint
  python main.py --start-step 3 --resume
        """,
    )

    parser.add_argument(
        "--start-step",
        type=int,
        choices=[1, 2, 3],
        default=1,
        help="Step to start from (1-3)",
    )
    parser.add_argument(
        "--end-step",
        type=int,
        choices=[1, 2, 3],
        default=3,
        help="Step to end at (1-3)",
    )
    parser.add_argument(
        "--word-range",
        type=str,
        help="Word index range for Step 2, e.g., '0-100'. Step 3 processes all enriched words.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=f"Process only {config.DRY_RUN_LIMIT} words for testing",
    )

    args = parser.parse_args()

    # Validate step range
    if args.start_step > args.end_step:
        print("Error: start-step cannot be greater than end-step")
        sys.exit(1)

    # Parse word range if provided
    word_range = None
    if args.word_range:
        try:
            word_range = parse_word_range(args.word_range)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    # Generate output path with timestamp at pipeline start
    pipeline_start = datetime.now()
    output_path = config.get_final_output_path(pipeline_start)

    print("=" * 60)
    print("DailyWord Data Generation Pipeline")
    print("=" * 60)
    print(f"Steps: {args.start_step} to {args.end_step}")
    if word_range:
        print(f"Word range: {word_range[0]} to {word_range[1]}")
    if args.resume:
        print("Mode: Resume from checkpoint")
    if args.dry_run:
        print(f"Mode: Dry run ({config.DRY_RUN_LIMIT} words)")
    print(f"Output: {output_path}")
    print("=" * 60)
    print()

    try:
        # Step 1: Word Selection
        if args.start_step <= 1 <= args.end_step:
            run_step1()
            print()

        # Step 2: Word Enrichment
        if args.start_step <= 2 <= args.end_step:
            run_step2(word_range=word_range, resume=args.resume, dry_run=args.dry_run)
            print()

        # Step 3: LLM Example Generation
        if args.start_step <= 3 <= args.end_step:
            run_step3(
                output_path=output_path,
                resume=args.resume,
                dry_run=args.dry_run,
            )
            print()

        print("=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        print("Progress has been saved. Use --resume to continue.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        print("Progress has been saved. Use --resume to continue.")
        sys.exit(1)


if __name__ == "__main__":
    main()
