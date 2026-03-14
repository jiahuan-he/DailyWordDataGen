"""Clean pipeline state: clear CSV output_file column and remove generated data."""

import argparse
import csv
import os
import shutil
import tempfile

import config


def count_csv_entries(csv_path=config.VOCABULARY_CSV):
    """Count rows with non-empty output_file."""
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return sum(1 for row in reader if row.get("output_file", "").strip())


def clear_csv_output_files(csv_path=config.VOCABULARY_CSV):
    """Clear all output_file values in CSV. Returns number of entries cleared."""
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    cleared = 0
    for row in rows:
        if row.get("output_file", "").strip():
            row["output_file"] = ""
            cleared += 1

    # Atomic write
    fd, tmp_path = tempfile.mkstemp(dir=csv_path.parent, suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, csv_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return cleared


def count_data_dirs(data_dir=config.FINAL_DATA_DIR):
    """Count subdirectories/files inside data dir."""
    if not data_dir.exists():
        return 0
    return len(list(data_dir.iterdir()))


def remove_data_contents(data_dir=config.FINAL_DATA_DIR):
    """Remove all contents of data dir, keeping the directory itself. Returns count removed."""
    if not data_dir.exists():
        return 0

    entries = list(data_dir.iterdir())
    for entry in entries:
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()

    return len(entries)


def main():
    parser = argparse.ArgumentParser(description="Clean pipeline state")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be cleaned")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    csv_entries = count_csv_entries()
    data_entries = count_data_dirs()

    if csv_entries == 0 and data_entries == 0:
        print("Nothing to clean.")
        return

    print(f"CSV entries to clear:        {csv_entries}")
    print(f"Data directories to remove:  {data_entries}")

    if args.dry_run:
        print("\n[dry-run] No changes made.")
        return

    if not args.yes:
        answer = input("\nProceed? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    cleared = clear_csv_output_files()
    removed = remove_data_contents()

    print(f"\nCleaned {cleared} CSV entries and removed {removed} data directories.")


if __name__ == "__main__":
    main()
