#!/usr/bin/env python3
"""DailyWord Data Generation Pipeline - Main Orchestrator."""

import argparse
import sys
from datetime import datetime

import config
from src.logger import setup_logger
from src.step2_enrichment import run_step2
from src.step3_generation import run_step3


def parse_range(range_str: str) -> tuple[int, int]:
    """Parse range string like '1-100' into tuple."""
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

  # Process specific word range (row indices, for parallel batching)
  python main.py --word-range 0-100

  # Resume from checkpoint
  python main.py --resume

  # Parallel execution (in separate terminals)
  python main.py --word-range 200-300
  python main.py --word-range 300-400
        """,
    )

    parser.add_argument(
        "--word-range",
        type=str,
        help="Word range, e.g., '0-100'. Filters words by row index (0-based). Uses separate checkpoint/output files for parallel execution.",
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

    # Set up logging
    logger = setup_logger()

    # Parse word range if provided
    word_range = None
    if args.word_range:
        try:
            word_range = parse_range(args.word_range)
        except ValueError as e:
            logger.error(f"Invalid word range: {e}")
            sys.exit(1)

    # Generate output path with timestamp at pipeline start
    pipeline_start = datetime.now()
    output_path = config.get_final_output_path(pipeline_start, word_range=word_range)

    logger.info("=" * 60)
    logger.info("DailyWord Data Generation Pipeline")
    logger.info("=" * 60)
    if word_range:
        logger.info(f"Word range: {word_range[0]} to {word_range[1]}")
    if args.resume:
        logger.info("Mode: Resume from checkpoint")
    if args.dry_run:
        logger.info(f"Mode: Dry run ({config.DRY_RUN_LIMIT} words)")
    logger.info(f"Output: {output_path}")
    logger.info("=" * 60)

    try:
        # Step 2: Word Enrichment
        run_step2(word_range=word_range, resume=args.resume, dry_run=args.dry_run)

        # Step 3: LLM Example Generation
        run_step3(
            output_path=output_path,
            word_range=word_range,
            resume=args.resume,
            dry_run=args.dry_run,
        )

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        logger.info("Progress has been saved. Use --resume to continue.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        logger.info("Progress has been saved. Use --resume to continue.")
        sys.exit(1)


if __name__ == "__main__":
    main()
