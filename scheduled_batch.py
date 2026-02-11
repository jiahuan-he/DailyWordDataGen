#!/usr/bin/env python3
"""Scheduler wrapper for batch_process.py.

Runs batch_process.py on a configurable schedule with:
- Start time, end time, interval
- Starting batch index
- Number of batches per interval
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
    """Calculate all scheduled run times.

    Args:
        start_time: First scheduled run time
        end_time: No new runs start after this time
        interval_hours: Hours between scheduled runs

    Returns:
        List of scheduled run times
    """
    run_times = []
    current = start_time
    while current <= end_time:
        run_times.append(current)
        current += timedelta(hours=interval_hours)
    return run_times


def find_current_bucket_index(run_times: list[datetime], now: datetime) -> int:
    """Find which time bucket we're currently in.

    If now is before first run, returns 0 (will wait).
    If now is between run[i] and run[i+1], returns i (execute run i immediately).
    If now is after the last run, returns len(run_times) (all past).

    Args:
        run_times: List of scheduled run times
        now: Current time

    Returns:
        Index of the current bucket, or len(run_times) if all runs are past
    """
    if not run_times:
        return 0

    # Before first run - return 0 (will wait for it)
    if now < run_times[0]:
        return 0

    # Find the bucket we're in: latest run whose time has passed
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


def run_batch_process(start_batch: int, batch_count: int, force: bool, logger) -> tuple[bool, int]:
    """Execute batch_process.py with given parameters.

    Args:
        start_batch: Starting batch index
        batch_count: Number of batches to process
        force: Whether to pass --force flag
        logger: Logger instance

    Returns:
        Tuple of (success, return_code)
    """
    cmd = ["python", "batch_process.py", str(start_batch), "--count", str(batch_count)]
    if force:
        cmd.append("--force")

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
  # Run batches 20-29, 30-39, 40-49, 50-59 at 5pm, 10pm, 3am, 8am
  python scheduled_batch.py \\
    --start-time "2025-02-10 17:00" \\
    --end-time "2025-02-11 09:00" \\
    --interval 5 \\
    --start-batch 20 \\
    --batch-count 10
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
        "--start-batch",
        type=int,
        required=True,
        help="First batch index to process"
    )
    parser.add_argument(
        "--batch-count",
        type=int,
        required=True,
        help="Number of batches to process per run"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Pass --force to batch_process.py"
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
    logger.info(f"Starting batch: {args.start_batch}")
    logger.info(f"Batches per run: {args.batch_count}")
    logger.info(f"Force mode: {args.force}")
    logger.info(f"Total scheduled runs: {len(run_times)}")
    logger.info("-" * 40)
    logger.info("Scheduled run times:")
    for i, rt in enumerate(run_times):
        batch_start = args.start_batch + (i * args.batch_count)
        batch_end = batch_start + args.batch_count - 1
        logger.info(f"  Run {i + 1}: {rt.strftime('%Y-%m-%d %H:%M')} - Batches {batch_start}-{batch_end}")
    logger.info("=" * 60)

    # Check if all runs are in the past (current time is past the end_time)
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
    skipped_runs = current_run_index  # Runs before the current bucket

    logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    if current_run_index > 0:
        logger.info(f"Starting from run {current_run_index + 1} (in current time bucket)")

    # Main scheduling loop
    while current_run_index < len(run_times):
        scheduled_time = run_times[current_run_index]
        now = datetime.now()

        # Calculate batch indices for this run
        batch_start = args.start_batch + (current_run_index * args.batch_count)

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
        logger.info(f"Processing batches {batch_start} to {batch_start + args.batch_count - 1}")

        # Execute batch_process.py
        success, return_code = run_batch_process(batch_start, args.batch_count, args.force, logger)

        run_end_time = datetime.now()
        run_duration = (run_end_time - run_start_time).total_seconds()

        if success:
            logger.info(f"Run {current_run_index + 1} completed successfully in {format_duration(run_duration)}")
            successful_runs += 1
        else:
            logger.warning(f"Run {current_run_index + 1} failed (return code: {return_code}) after {format_duration(run_duration)}")
            failed_runs += 1
            # Continue to next run despite failure

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
