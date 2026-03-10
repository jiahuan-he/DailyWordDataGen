#!/usr/bin/env python3
"""Batch processor for DailyWord vocabulary data generation.

Repeatedly calls main.py --count N to process unprocessed words.
The CSV tracks progress — no range/folder logic needed.
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime

from src.logger import setup_logger
from src.step2_enrichment import load_unprocessed_words


def run_main(count: int, logger) -> bool:
    """Execute main.py --count N.

    Returns:
        True if successful, False if failed
    """
    cmd = ["python", "main.py", "--count", str(count)]
    logger.info(f"Executing: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd)
        if result.returncode == 0:
            logger.info("main.py completed successfully")
            return True
        else:
            logger.warning(f"main.py failed with return code {result.returncode}")
            return False
    except Exception as e:
        logger.error(f"Failed to execute main.py: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Batch processor: repeatedly runs main.py to process unprocessed words",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all remaining words, 100 at a time
  python batch_process.py

  # Process 3 batches of 50 words each
  python batch_process.py --count 50 --batches 3
        """,
    )
    parser.add_argument(
        "--count", "-n",
        type=int,
        default=100,
        help="Number of words per run (default: 100)",
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=None,
        help="Number of runs (default: unlimited, runs until all words are processed)",
    )

    args = parser.parse_args()

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"batch_{timestamp}.log"
    logger = setup_logger(name="batch", log_file=log_file)

    logger.info("=" * 60)
    logger.info("DailyWord Batch Processing")
    logger.info("=" * 60)
    logger.info(f"Words per run: {args.count}")
    if args.batches is not None:
        logger.info(f"Max runs: {args.batches}")
    else:
        logger.info("Max runs: unlimited (until all words done)")
    logger.info("=" * 60)

    completed_runs = 0
    failed_runs = 0

    batch_num = 0
    while True:
        # Check batch limit
        if args.batches is not None and batch_num >= args.batches:
            logger.info(f"Reached batch limit ({args.batches}), stopping")
            break

        # Check if there are unprocessed words remaining
        remaining = load_unprocessed_words(1)
        if not remaining:
            logger.info("No unprocessed words remaining. All done!")
            break

        batch_num += 1
        logger.info("-" * 40)
        logger.info(f"Run {batch_num}" + (f"/{args.batches}" if args.batches else ""))

        success = run_main(args.count, logger)
        if success:
            completed_runs += 1
        else:
            failed_runs += 1
            logger.error("Run failed, stopping batch processing")
            break

        # Brief pause between runs
        if args.batches is None or batch_num < args.batches:
            logger.info("Waiting 5 seconds before next run...")
            time.sleep(5)

    # Summary
    logger.info("=" * 60)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Completed runs: {completed_runs}")
    logger.info(f"Failed runs: {failed_runs}")

    return 1 if failed_runs > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
