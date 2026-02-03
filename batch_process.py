#!/usr/bin/env python3
"""Batch processor for DailyWord vocabulary data generation.

Processes all words organized by frequency range (e.g., 1-100, 101-200, etc.).
"""

import csv
import glob
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Configuration
BATCH_SIZE = 100  # Frequency range per batch
MAX_FREQUENCY = 20000
MAX_RETRIES = 3
FINAL_DATA_DIR = Path("final_data")
DATA_DIR = Path("data")
CHECKPOINTS_DIR = Path("checkpoints")
SELECTED_WORDS_CSV = DATA_DIR / "selected_words.csv"


def clear_checkpoints():
    """Clear checkpoint files to ensure fresh processing for each batch."""
    for checkpoint_file in CHECKPOINTS_DIR.glob("*.json"):
        checkpoint_file.unlink()
        print(f"  Cleared checkpoint: {checkpoint_file.name}")


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


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"  Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ERROR: {description} failed with return code {e.returncode}")
        return False
    except Exception as e:
        print(f"  ERROR: {description} failed: {e}")
        return False


def batch_has_valid_output(batch_folder: Path, expected_word_count: int) -> bool:
    """Check if batch folder already has valid output."""
    json_files = list(batch_folder.glob("final_output_*.json"))
    if not json_files:
        return False

    # Check if the output file has actual content
    for json_file in json_files:
        try:
            import json
            with open(json_file, 'r') as f:
                data = json.load(f)
                # Consider valid if at least 50% of expected words are present
                if len(data) >= expected_word_count * 0.5:
                    return True
        except:
            pass
    return False


def process_batch(batch_index: int, frequencies: set[int], words_data: list[tuple[int, int, str]], force: bool = False) -> bool:
    """Process a single batch with retry logic.

    Args:
        batch_index: 0-based batch index
        frequencies: Set of all frequencies (for quick count check)
        words_data: List of (row_index, frequency, word) tuples for row range lookup
        force: Force reprocessing even if output exists

    Returns:
        True if successful (or skipped), False if failed after all retries
    """
    min_freq, max_freq, folder_name = get_batch_info(batch_index)
    batch_folder = FINAL_DATA_DIR / folder_name

    # Check if any words exist in this frequency range
    word_count = count_words_in_range(frequencies, min_freq, max_freq)
    if word_count == 0:
        print(f"\n[Batch {batch_index + 1}] {folder_name}: No words in range, skipping")
        return True

    # Check if batch already has valid output (skip if so, unless forced)
    if not force and batch_has_valid_output(batch_folder, word_count):
        print(f"\n[Batch {batch_index + 1}] {folder_name}: Already has valid output, skipping")
        return True

    # Convert frequency range to row range
    row_range = get_row_range_for_frequency(words_data, min_freq, max_freq)
    if row_range is None:
        print(f"\n[Batch {batch_index + 1}] {folder_name}: No words in frequency range, skipping")
        return True

    start_row, end_row = row_range

    print(f"\n{'='*60}")
    print(f"Processing batch {batch_index + 1}: {folder_name} ({word_count} words, rows {start_row}-{end_row})")
    print(f"{'='*60}")

    # Create batch folder
    batch_folder.mkdir(parents=True, exist_ok=True)

    # Clear checkpoints before each batch to ensure fresh processing
    clear_checkpoints()

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\nAttempt {attempt}/{MAX_RETRIES}")

        # Step 2: Enrich words in this row range
        step2_cmd = [
            "python", "main.py",
            "--start-step", "2", "--end-step", "2",
            "--word-range", f"{start_row}-{end_row}"
        ]
        if attempt > 1:
            step2_cmd.append("--resume")

        step2_success = run_command(step2_cmd, "Step 2 (enrichment)")

        if not step2_success:
            print(f"  Step 2 failed, retrying...")
            time.sleep(5)
            continue

        # Step 3: Generate examples (with resume on retry)
        step3_cmd = ["python", "main.py", "--start-step", "3", "--end-step", "3"]
        if attempt > 1:
            step3_cmd.append("--resume")

        step3_success = run_command(step3_cmd, "Step 3 (generation)")

        if not step3_success:
            print(f"  Step 3 failed, retrying...")
            time.sleep(5)
            continue

        # Move output files to batch folder
        output_files = glob.glob(str(DATA_DIR / "final_output_*.json"))
        if not output_files:
            print("  WARNING: No output file found!")
            continue

        for output_file in output_files:
            dest = batch_folder / Path(output_file).name
            shutil.move(output_file, dest)
            print(f"  Moved: {output_file} -> {dest}")

        print(f"  Batch {folder_name} completed successfully!")
        return True

    print(f"  FAILED: Batch {folder_name} failed after {MAX_RETRIES} attempts")
    return False


def main():
    """Main batch processing loop."""
    # Calculate total batches
    total_batches = (MAX_FREQUENCY + BATCH_SIZE - 1) // BATCH_SIZE

    print("=" * 60)
    print("DailyWord Batch Processing (by Frequency)")
    print("=" * 60)
    print(f"Frequency range: 1 to {MAX_FREQUENCY}")
    print(f"Batch size: {BATCH_SIZE} frequencies per batch")
    print(f"Total batches: {total_batches}")
    print(f"Output directory: {FINAL_DATA_DIR}")

    # Load frequencies to check which ranges have words
    print("\nLoading word frequencies...")
    frequencies = load_frequency_set()
    words_data = load_words_with_indices()
    print(f"Total unique frequencies: {len(frequencies)}")
    print(f"Total words loaded: {len(words_data)}")
    print("=" * 60)

    # Create main output directory
    FINAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Track results
    failed_batches = []
    skipped_batches = 0
    processed_batches = 0

    # Parse command line for starting batch and force flag
    start_batch = 0
    force = False
    for arg in sys.argv[1:]:
        if arg == "--force":
            force = True
            print("\nForce mode: will reprocess all batches")
        else:
            try:
                start_batch = int(arg)
                print(f"\nStarting from batch {start_batch}")
            except ValueError:
                print(f"Usage: {sys.argv[0]} [start_batch_index] [--force]")
                sys.exit(1)

    # Process each batch
    for batch_index in range(start_batch, total_batches):
        min_freq, max_freq, _ = get_batch_info(batch_index)
        word_count = count_words_in_range(frequencies, min_freq, max_freq)

        if word_count == 0:
            skipped_batches += 1
            continue

        success = process_batch(batch_index, frequencies, words_data, force=force)
        if success:
            processed_batches += 1
        else:
            failed_batches.append(batch_index)

        # Brief pause between batches
        time.sleep(2)

    # Summary
    print("\n" + "=" * 60)
    print("BATCH PROCESSING COMPLETE")
    print("=" * 60)
    print(f"Processed: {processed_batches} batches")
    print(f"Skipped (empty): {skipped_batches} batches")
    print(f"Failed: {len(failed_batches)} batches")

    if failed_batches:
        print(f"\nFailed batches:")
        for batch_idx in failed_batches:
            _, _, folder_name = get_batch_info(batch_idx)
            print(f"  - Batch {batch_idx}: {folder_name}")
        print("\nTo retry failed batches, run:")
        for batch_idx in failed_batches:
            print(f"  python batch_process.py {batch_idx}")
    else:
        print("\nAll batches completed successfully!")

    # Verification
    print("\n" + "-" * 40)
    print("Verification:")
    folder_count = len(list(FINAL_DATA_DIR.iterdir())) if FINAL_DATA_DIR.exists() else 0
    print(f"  Output folders created: {folder_count}")

    return 0 if not failed_batches else 1


if __name__ == "__main__":
    sys.exit(main())
