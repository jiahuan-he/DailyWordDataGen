#!/usr/bin/env python3
"""Batch processor for DailyWord vocabulary data generation.

Processes all words organized by frequency range (e.g., 1-100, 101-200, etc.).
"""

import csv
import glob
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from src.logger import setup_logger

# Configuration
BATCH_SIZE = 100  # Frequency range per batch
MAX_FREQUENCY = 20000
MAX_RETRIES = 3
FINAL_DATA_DIR = Path("final_data")
DATA_DIR = Path("data")
CHECKPOINTS_DIR = Path("checkpoints")
SELECTED_WORDS_CSV = DATA_DIR / "selected_words.csv"


def clear_checkpoints(logger):
    """Clear checkpoint files to ensure fresh processing for each batch."""
    for checkpoint_file in CHECKPOINTS_DIR.glob("*.json"):
        checkpoint_file.unlink()
        logger.debug(f"Cleared checkpoint: {checkpoint_file.name}")


def load_frequency_set() -> set[int]:
    """Load all frequencies from selected_words.csv."""
    frequencies = set()
    with open(SELECTED_WORDS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            frequencies.add(int(row["frequency"]))
    return frequencies


def load_words_with_indices() -> list[tuple[int, int, str]]:
    """Load words with their row index and frequency.

    Returns:
        List of (row_index, frequency, word) tuples
    """
    words = []
    with open(SELECTED_WORDS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            words.append((idx, int(row["frequency"]), row["word"]))
    return words


def get_row_range_for_frequency(words: list[tuple[int, int, str]], min_freq: int, max_freq: int) -> tuple[int, int] | None:
    """Find the row index range for words within a frequency range.

    Args:
        words: List of (row_index, frequency, word) tuples
        min_freq: Minimum frequency (inclusive)
        max_freq: Maximum frequency (inclusive)

    Returns:
        (start_row, end_row) tuple where end is exclusive, or None if no words in range
    """
    matching_indices = [idx for idx, freq, _ in words if min_freq <= freq <= max_freq]
    if not matching_indices:
        return None
    return min(matching_indices), max(matching_indices) + 1  # end is exclusive


def get_batch_info(batch_index: int) -> tuple[int, int, str]:
    """Calculate batch frequency range and folder name.

    Args:
        batch_index: 0-based batch index (0, 1, 2, ...)

    Returns:
        Tuple of (min_freq, max_freq, folder_name)
    """
    min_freq = batch_index * BATCH_SIZE + 1
    max_freq = (batch_index + 1) * BATCH_SIZE
    folder_name = f"{min_freq}-{max_freq}"
    return min_freq, max_freq, folder_name


def count_words_in_range(frequencies: set[int], min_freq: int, max_freq: int) -> int:
    """Count how many words fall within a frequency range."""
    return sum(1 for f in frequencies if min_freq <= f <= max_freq)


def run_command(cmd: list[str], description: str, logger) -> tuple[bool, str]:
    """Run a command and return success status and output."""
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.debug(f"Command stdout: {result.stdout[-500:] if result.stdout else 'empty'}")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"{description} failed with return code {e.returncode}")
        logger.error(f"Stderr: {e.stderr}")
        return False, e.stderr
    except Exception as e:
        logger.error(f"{description} failed: {e}")
        return False, str(e)


def batch_has_valid_output(batch_folder: Path, expected_word_count: int, logger) -> bool:
    """Check if batch folder already has valid output."""
    json_files = list(batch_folder.glob("final_output_*.json"))
    if not json_files:
        return False

    # Check if the output file has actual content
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                # Consider valid if at least 50% of expected words are present
                if len(data) >= expected_word_count * 0.5:
                    logger.debug(f"Found valid output: {json_file} with {len(data)} words")
                    return True
                else:
                    logger.debug(f"Output file {json_file} has only {len(data)} words (expected {expected_word_count})")
        except Exception as e:
            logger.warning(f"Error reading {json_file}: {e}")
    return False


def is_valid_output_file(file_path: Path, min_words: int = 1) -> tuple[bool, int]:
    """Check if an output file has valid content.

    Returns:
        Tuple of (is_valid, word_count)
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return len(data) >= min_words, len(data)
    except Exception:
        return False, 0


def process_batch(batch_index: int, frequencies: set[int], words_data: list[tuple[int, int, str]], logger, force: bool = False) -> bool:
    """Process a single batch with retry logic.

    Args:
        batch_index: 0-based batch index
        frequencies: Set of all frequencies (for quick count check)
        words_data: List of (row_index, frequency, word) tuples for row range lookup
        logger: Logger instance
        force: Force reprocessing even if output exists

    Returns:
        True if successful (or skipped), False if failed after all retries
    """
    min_freq, max_freq, folder_name = get_batch_info(batch_index)
    batch_folder = FINAL_DATA_DIR / folder_name

    # Check if any words exist in this frequency range
    word_count = count_words_in_range(frequencies, min_freq, max_freq)
    if word_count == 0:
        logger.info(f"[Batch {batch_index}] {folder_name}: No words in range, skipping")
        return True

    # Check if batch already has valid output (skip if so, unless forced)
    if not force and batch_has_valid_output(batch_folder, word_count, logger):
        logger.info(f"[Batch {batch_index}] {folder_name}: Already has valid output, skipping")
        return True

    # Convert frequency range to row range
    row_range = get_row_range_for_frequency(words_data, min_freq, max_freq)
    if row_range is None:
        logger.info(f"[Batch {batch_index}] {folder_name}: No words in frequency range, skipping")
        return True

    start_row, end_row = row_range

    logger.info("=" * 60)
    logger.info(f"Processing batch {batch_index}: {folder_name} ({word_count} words, rows {start_row}-{end_row})")
    logger.info("=" * 60)

    # Create batch folder
    batch_folder.mkdir(parents=True, exist_ok=True)

    # Clear checkpoints before each batch to ensure fresh processing
    clear_checkpoints(logger)

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Attempt {attempt}/{MAX_RETRIES}")

        # Step 2: Enrich words in this row range
        step2_cmd = [
            "python", "main.py",
            "--start-step", "2", "--end-step", "2",
            "--word-range", f"{start_row}-{end_row}"
        ]
        if attempt > 1:
            step2_cmd.append("--resume")

        step2_success, step2_output = run_command(step2_cmd, "Step 2 (enrichment)", logger)

        if not step2_success:
            logger.warning(f"Step 2 failed, retrying in 5 seconds...")
            time.sleep(5)
            continue

        # Step 3: Generate examples (with resume on retry)
        step3_cmd = ["python", "main.py", "--start-step", "3", "--end-step", "3"]
        if attempt > 1:
            step3_cmd.append("--resume")

        step3_success, step3_output = run_command(step3_cmd, "Step 3 (generation)", logger)

        if not step3_success:
            logger.warning(f"Step 3 failed, retrying in 5 seconds...")
            time.sleep(5)
            continue

        # Move output files to batch folder (only if valid)
        output_files = glob.glob(str(DATA_DIR / "final_output_*.json"))
        if not output_files:
            logger.warning("No output file found!")
            continue

        moved_valid = False
        for output_file in output_files:
            output_path = Path(output_file)
            is_valid, file_word_count = is_valid_output_file(output_path, min_words=1)

            if is_valid:
                dest = batch_folder / output_path.name
                shutil.move(output_file, dest)
                logger.info(f"Moved: {output_file} -> {dest} ({file_word_count} words)")
                moved_valid = True
            else:
                # Delete empty output files
                output_path.unlink()
                logger.warning(f"Deleted empty output file: {output_file}")

        if moved_valid:
            logger.info(f"Batch {folder_name} completed successfully!")
            return True
        else:
            logger.warning(f"No valid output produced, retrying...")
            time.sleep(5)
            continue

    logger.error(f"FAILED: Batch {folder_name} failed after {MAX_RETRIES} attempts")
    return False


def main():
    """Main batch processing loop."""
    # Parse command line for starting batch and force flag
    start_batch = 0
    force = False
    for arg in sys.argv[1:]:
        if arg == "--force":
            force = True
        else:
            try:
                start_batch = int(arg)
            except ValueError:
                print(f"Usage: {sys.argv[0]} [start_batch_index] [--force]")
                sys.exit(1)

    # Set up logging with batch-specific log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"batch_from_{start_batch}_{timestamp}.log"
    logger = setup_logger(name="batch", log_file=log_file)

    # Calculate total batches
    total_batches = (MAX_FREQUENCY + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info("=" * 60)
    logger.info("DailyWord Batch Processing (by Frequency)")
    logger.info("=" * 60)
    logger.info(f"Frequency range: 1 to {MAX_FREQUENCY}")
    logger.info(f"Batch size: {BATCH_SIZE} frequencies per batch")
    logger.info(f"Total batches: {total_batches}")
    logger.info(f"Output directory: {FINAL_DATA_DIR}")
    logger.info(f"Starting from batch: {start_batch}")
    if force:
        logger.info("Force mode: will reprocess all batches")

    # Load frequencies to check which ranges have words
    logger.info("Loading word frequencies...")
    frequencies = load_frequency_set()
    words_data = load_words_with_indices()
    logger.info(f"Total unique frequencies: {len(frequencies)}")
    logger.info(f"Total words loaded: {len(words_data)}")
    logger.info("=" * 60)

    # Create main output directory
    FINAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Track results
    failed_batches = []
    skipped_batches = 0
    processed_batches = 0

    # Process each batch
    stopped_early = False
    last_batch_index = start_batch

    for batch_index in range(start_batch, total_batches):
        last_batch_index = batch_index
        min_freq, max_freq, folder_name = get_batch_info(batch_index)
        word_count = count_words_in_range(frequencies, min_freq, max_freq)

        if word_count == 0:
            skipped_batches += 1
            continue

        success = process_batch(batch_index, frequencies, words_data, logger, force=force)
        if success:
            processed_batches += 1
        else:
            failed_batches.append(batch_index)
            # Stop processing immediately on failure
            logger.error("=" * 60)
            logger.error(f"STOPPING: Batch {batch_index} ({folder_name}) failed to produce valid output")
            logger.error("This likely indicates a systemic issue (rate limiting, API errors, etc.)")
            logger.error(f"To resume from this batch, run: python batch_process.py {batch_index}")
            logger.error("=" * 60)
            stopped_early = True
            break

        # Brief pause between batches (increased to 5 seconds to avoid rate limiting)
        logger.info("Waiting 5 seconds before next batch...")
        time.sleep(5)

    # Summary
    logger.info("=" * 60)
    if stopped_early:
        logger.info("BATCH PROCESSING STOPPED (due to failure)")
    else:
        logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Processed: {processed_batches} batches")
    logger.info(f"Skipped (empty): {skipped_batches} batches")
    logger.info(f"Failed: {len(failed_batches)} batches")

    if failed_batches:
        logger.warning("Failed batches:")
        for batch_idx in failed_batches:
            _, _, folder_name = get_batch_info(batch_idx)
            logger.warning(f"  - Batch {batch_idx}: {folder_name}")
        logger.info("To resume, run:")
        logger.info(f"  python batch_process.py {failed_batches[0]}")
    else:
        logger.info("All batches completed successfully!")

    # Verification
    logger.info("-" * 40)
    logger.info("Verification:")
    folder_count = len(list(FINAL_DATA_DIR.iterdir())) if FINAL_DATA_DIR.exists() else 0
    logger.info(f"  Output folders created: {folder_count}")

    return 0 if not failed_batches else 1


if __name__ == "__main__":
    sys.exit(main())
