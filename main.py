#!/usr/bin/env python3
"""DailyWord Data Generation Pipeline - Main Orchestrator."""

import argparse
import sys
from datetime import datetime

import config
from src.logger import setup_logger
from src.step2_enrichment import load_unprocessed_words
from src.step3_generation import run_step3


def main():
    parser = argparse.ArgumentParser(
        description="DailyWord Data Generation Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with 10 words (saves to test_output/, does not update CSV)
  python main.py --dry-run

  # Process 50 unprocessed words
  python main.py --count 50

  # Process next 100 unprocessed words (default count)
  python main.py

  # Test with a different model (output goes to test_output/)
  python main.py --test --model claude-sonnet-4-5-20250514 --count 20

  # Process 10 words each from frequency tiers 2, 3, and 5
  python main.py --count 10 --frequencies 2,3,5
        """,
    )

    parser.add_argument(
        "--count", "-n",
        type=int,
        default=config.DEFAULT_BATCH_SIZE,
        help=f"Number of unprocessed words to process (default: {config.DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=f"Process only {config.DRY_RUN_LIMIT} words for testing (saves to test_output/, no CSV update)",
    )
    parser.add_argument(
        "--model",
        type=str,
        help="Override Claude model (e.g., claude-sonnet-4-5-20250514)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Route output to test_output/<model>/ instead of final_data_v4/ (no CSV update)",
    )
    parser.add_argument(
        "--frequencies", "-f",
        type=str,
        default=None,
        help="Comma-separated frequency tiers to filter by (e.g., 2,3,5). "
             "Loads --count words per frequency tier.",
    )

    args = parser.parse_args()

    # Parse frequencies
    frequencies = None
    if args.frequencies:
        frequencies = [int(f.strip()) for f in args.frequencies.split(",")]

    # Override model if specified
    if args.model:
        config.CLAUDE_MODEL = args.model

    # Set up logging
    logger = setup_logger()

    # Determine mode
    test_mode = args.test or args.dry_run
    count = config.DRY_RUN_LIMIT if args.dry_run else args.count
    model_short = config.model_short_name(config.CLAUDE_MODEL)

    # Load unprocessed words
    words = load_unprocessed_words(count, frequencies=frequencies)
    if not words:
        logger.info("No unprocessed words remaining. Nothing to do.")
        sys.exit(0)

    pipeline_start = datetime.now()

    logger.info("=" * 60)
    logger.info("DailyWord Data Generation Pipeline")
    logger.info("=" * 60)
    if test_mode:
        logger.info(f"TEST MODE — model: {config.CLAUDE_MODEL} ({model_short})")
    if frequencies:
        logger.info(f"Frequency filter: {frequencies} ({count} per tier)")
    logger.info(f"Words to process: {len(words)}")
    if args.dry_run:
        logger.info(f"Mode: Dry run ({config.DRY_RUN_LIMIT} words)")
    logger.info("=" * 60)

    try:
        # Process words: enrich + generate examples (per-word save + CSV update)
        run_step3(
            words=words,
            timestamp=pipeline_start,
            test_mode=test_mode,
            model_short=model_short,
        )

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        logger.info("Already-saved words are safe. Re-run to continue with remaining words.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        logger.info("Already-saved words are safe. Re-run to continue with remaining words.")
        sys.exit(1)


if __name__ == "__main__":
    main()
