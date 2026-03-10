#!/usr/bin/env python3
"""Scheduler wrapper for batch_process.py.

Runs batch_process.py on a configurable schedule with:
- Start time, end time, interval
- Number of batches per scheduled run
- Words per batch
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta

from src.logger import setup_logger


def parse_datetime(dt_string: str) -> datetime:
    """Parse datetime string in 'YYYY-MM-DD HH:MM' format."""
    return datetime.strptime(dt_string, "%Y-%m-%d %H:%M")


def calculate_run_times(start_time: datetime, end_time: datetime, interval_hours: float) -> list[datetime]:
    """Calculate all scheduled run times."""
    run_times = []
    current = start_time
    while current <= end_time:
        run_times.append(current)
        current += timedelta(hours=interval_hours)
    return run_times


def find_current_bucket_index(run_times: list[datetime], now: datetime) -> int:
    """Find which time bucket we're currently in."""
    if not run_times:
        return 0
    if now < run_times[0]:
        return 0
    for i in range(len(run_times) - 1, -1, -1):
        if now >= run_times[i]:
            return i
    return 0


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"


def run_batch_process(count: int, batches: int, logger) -> tuple[bool, int]:
    """Execute batch_process.py with given parameters.

    Args:
        count: Words per batch run
        batches: Number of batch runs
        logger: Logger instance

    Returns:
        Tuple of (success, return_code)
    """
    cmd = ["python", "batch_process.py", "--count", str(count), "--batches", str(batches)]

    logger.info(f"Executing: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd)
        if result.returncode == 0:
            logger.info("batch_process.py completed successfully")
            return True, 0
        else:
            logger.warning(f"batch_process.py failed with return code {result.returncode}")
            return False, result.returncode
    except Exception as e:
        logger.error(f"Failed to execute batch_process.py: {e}")
        return False, -1


def main():
    """Main scheduler loop."""
    parser = argparse.ArgumentParser(
        description="Scheduler wrapper for batch_process.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  # Run 10 batches of 100 words at 5pm, 10pm, 3am, 8am
  python scheduled_batch.py \\
    --start-time "2025-02-10 17:00" \\
    --end-time "2025-02-11 09:00" \\
    --interval 5 \\
    --count 100 \\
    --batches-per-run 10
        """
    )
    parser.add_argument(
        "--start-time",
        required=True,
        help="Start time in 'YYYY-MM-DD HH:MM' format"
    )
    parser.add_argument(
        "--end-time",
        required=True,
        help="End time in 'YYYY-MM-DD HH:MM' format"
    )
    parser.add_argument(
        "--interval",
        type=float,
        required=True,
        help="Interval between runs in hours"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Words per batch (passed to batch_process.py --count, default: 100)"
    )
    parser.add_argument(
        "--batches-per-run",
        type=int,
        required=True,
        help="Number of batch_process.py runs per scheduled time"
    )

    args = parser.parse_args()

    # Parse times
    try:
        start_time = parse_datetime(args.start_time)
        end_time = parse_datetime(args.end_time)
    except ValueError as e:
        print(f"Error parsing time: {e}")
        print("Expected format: 'YYYY-MM-DD HH:MM'")
        sys.exit(1)

    if end_time <= start_time:
        print("Error: end-time must be after start-time")
        sys.exit(1)

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"scheduler_{timestamp}.log"
    logger = setup_logger(name="scheduler", log_file=log_file)

    # Calculate all run times
    run_times = calculate_run_times(start_time, end_time, args.interval)

    logger.info("=" * 60)
    logger.info("DailyWord Scheduled Batch Processing")
    logger.info("=" * 60)
    logger.info(f"Schedule: {args.start_time} to {args.end_time}")
    logger.info(f"Interval: {args.interval} hours")
    logger.info(f"Words per batch: {args.count}")
    logger.info(f"Batches per run: {args.batches_per_run}")
    logger.info(f"Total scheduled runs: {len(run_times)}")
    logger.info("-" * 40)
    logger.info("Scheduled run times:")
    for i, rt in enumerate(run_times):
        logger.info(f"  Run {i + 1}: {rt.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 60)

    # Check if all runs are in the past
    now = datetime.now()
    if now > end_time:
        logger.warning("All scheduled runs are in the past. Nothing to do.")
        logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Schedule ended at: {end_time.strftime('%Y-%m-%d %H:%M')}")
        sys.exit(0)

    # Find which bucket we're currently in
    current_run_index = find_current_bucket_index(run_times, now)

    # Track results
    successful_runs = 0
    failed_runs = 0
    skipped_runs = current_run_index

    logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    if current_run_index > 0:
        logger.info(f"Starting from run {current_run_index + 1} (in current time bucket)")

    # Main scheduling loop
    while current_run_index < len(run_times):
        scheduled_time = run_times[current_run_index]
        now = datetime.now()

        # Wait if we're early, otherwise start immediately
        if now < scheduled_time:
            wait_seconds = (scheduled_time - now).total_seconds()
            logger.info(f"Waiting {format_duration(wait_seconds)} until run {current_run_index + 1} at {scheduled_time.strftime('%Y-%m-%d %H:%M')}")
            time.sleep(wait_seconds)
        else:
            logger.info(f"Scheduled time {scheduled_time.strftime('%Y-%m-%d %H:%M')} has passed, starting immediately")

        # Log run start
        run_start_time = datetime.now()
        logger.info("-" * 40)
        logger.info(f"Run {current_run_index + 1}/{len(run_times)} starting at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Execute batch_process.py
        success, return_code = run_batch_process(args.count, args.batches_per_run, logger)

        run_end_time = datetime.now()
        run_duration = (run_end_time - run_start_time).total_seconds()

        if success:
            logger.info(f"Run {current_run_index + 1} completed successfully in {format_duration(run_duration)}")
            successful_runs += 1
        else:
            logger.warning(f"Run {current_run_index + 1} failed (return code: {return_code}) after {format_duration(run_duration)}")
            failed_runs += 1

        # Move to next run
        current_run_index += 1

        # Skip any runs whose scheduled time has passed while we were running
        now = datetime.now()
        while current_run_index < len(run_times) and now >= run_times[current_run_index]:
            skipped_time = run_times[current_run_index]
            logger.warning(f"Skipping run {current_run_index + 1} (scheduled for {skipped_time.strftime('%Y-%m-%d %H:%M')}) - time has passed")
            skipped_runs += 1
            current_run_index += 1

    # Summary
    logger.info("=" * 60)
    logger.info("SCHEDULED BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Successful runs: {successful_runs}")
    logger.info(f"Failed runs: {failed_runs}")
    logger.info(f"Skipped runs: {skipped_runs}")
    logger.info(f"Total scheduled: {len(run_times)}")

    if failed_runs > 0:
        logger.warning("Some runs failed. Check batch_process.py logs for details.")
        sys.exit(1)

    logger.info("All runs completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
