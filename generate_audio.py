#!/usr/bin/env python3
"""Generate TTS audio for DailyWord words and sentences using MiniMax API."""

import argparse
import csv
import json
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

import config
from src.minimax_client import (
    create_client,
    generate_audio,
    get_api_key,
    MiniMaxTTSError,
)

AUDIO_FILES = (
    ["word.mp3"]
    + [f"sentence_{i}.mp3" for i in range(1, config.EXAMPLES_PER_WORD + 1)]
    + ["metadata.json"]
)


# ── CSV Tracker ─────────────────────────────────────────────


def _load_main_csv() -> dict[str, dict]:
    """Load word_frequencies_sorted_v4.csv into {word: {frequency, output_file}} dict."""
    words = {}
    with open(config.VOCABULARY_CSV, "r", newline="") as f:
        for row in csv.DictReader(f):
            word = row["word"].strip()
            if word:
                words[word] = {
                    "frequency": int(float(row.get("frequency", 0) or 0)),
                    "output_file": row.get("output_file", "").strip(),
                }
    return words


def _voice_columns() -> list[str]:
    """Return sorted list of voice keys from config."""
    return sorted(config.VOICES.keys())


def init_tracker_csv() -> Path:
    """Initialize or sync audio_generation_tracker.csv.

    Creates it from word_frequencies_sorted_v4.csv if missing.
    Adds new voice columns if voices were added to config.
    """
    tracker_path = config.AUDIO_TRACKER_CSV
    voice_cols = _voice_columns()

    if tracker_path.exists():
        with open(tracker_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)

        changed = False

        # Add missing voice columns
        missing_cols = [v for v in voice_cols if v not in fieldnames]
        if missing_cols:
            fieldnames = fieldnames + missing_cols
            for row in rows:
                for col in missing_cols:
                    row[col] = ""
            changed = True

        # Add new words from main CSV
        main_words = _load_main_csv()
        tracker_words = {row["word"] for row in rows}
        new_words = [w for w in main_words if w not in tracker_words]

        if new_words:
            for word in new_words:
                row = {"word": word, "frequency": str(main_words[word]["frequency"])}
                for v in voice_cols:
                    row[v] = ""
                rows.append(row)
            changed = True

        if changed:
            _write_tracker_csv(tracker_path, fieldnames, rows)

        return tracker_path

    # Create from scratch
    main_words = _load_main_csv()
    fieldnames = ["word", "frequency"] + voice_cols
    rows = []
    for word, info in main_words.items():
        row = {"word": word, "frequency": str(info["frequency"])}
        for v in voice_cols:
            row[v] = ""
        rows.append(row)

    _write_tracker_csv(tracker_path, fieldnames, rows)
    return tracker_path


def _write_tracker_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    """Write tracker CSV atomically."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_ungenerated_words(
    voice_key: str,
    count: int | None = None,
    frequencies: list[int] | None = None,
    specific_words: list[str] | None = None,
    main_words: dict[str, dict] | None = None,
) -> list[dict]:
    """Load words that haven't had audio generated for the given voice.

    Returns list of dicts with word and frequency keys, in CSV order.
    Only includes words that have word data (output_file in main CSV).
    """
    if main_words is None:
        main_words = _load_main_csv()

    with open(config.AUDIO_TRACKER_CSV, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    specific_set = set(specific_words) if specific_words is not None else None

    # Filter to ungenerated words with word data
    ungenerated = []
    for row in rows:
        word = row["word"]
        if not main_words.get(word, {}).get("output_file"):
            continue
        if row.get(voice_key, "").strip():
            continue
        if specific_set is not None and word not in specific_set:
            continue

        ungenerated.append({
            "word": word,
            "frequency": int(row.get("frequency", 0) or 0),
        })

    if frequencies is not None:
        freq_counts = {f: 0 for f in frequencies}
        selected_indices = set()
        for i, w in enumerate(ungenerated):
            if w["frequency"] in freq_counts and freq_counts[w["frequency"]] < (count or 100):
                selected_indices.add(i)
                freq_counts[w["frequency"]] += 1
        ungenerated = [ungenerated[i] for i in sorted(selected_indices)]
    elif count is not None:
        ungenerated = ungenerated[:count]

    return ungenerated


# ── Word Data Loading ───────────────────────────────────────


def load_word_data(word: str) -> dict:
    """Load the latest word JSON from final_data_v4/{word}/."""
    safe = config._safe_word(word)
    word_dir = config.FINAL_DATA_DIR / safe
    if not word_dir.is_dir():
        raise FileNotFoundError(f"No data directory for word: {word}")

    json_files = sorted(word_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files in {word_dir}")

    with open(json_files[-1]) as f:
        return json.load(f)


# ── Audio Generation ────────────────────────────────────────


def generate_word_audio(
    word_data: dict,
    voice_key: str,
    voice_id: str,
    api_key: str,
    client,
) -> tuple[bool, str]:
    """Generate all audio files for a single word.

    Returns (success, output_dir_or_error_message).
    """
    word = word_data["word"]
    safe = config._safe_word(word)
    output_dir = config.AUDIO_DATA_DIR / voice_key / safe
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build list of (filename, text) pairs
    audio_items = [("word.mp3", word)]

    examples = sorted(word_data["examples"], key=lambda e: e.get("display_order", 0))
    for ex in examples:
        order = ex.get("display_order")
        if order is not None:
            audio_items.append((f"sentence_{order}.mp3", ex["sentence"]))

    # Generate each audio file
    metadata_files = {}
    for filename, text in audio_items:
        try:
            audio_bytes, _ = generate_audio(api_key, voice_id, text, client=client)
        except Exception as e:
            return False, f"Failed on {filename}: {e}"

        filepath = output_dir / filename
        with open(filepath, "wb") as f:
            f.write(audio_bytes)

        metadata_files[filename] = {"text": text}

    # Write metadata.json
    metadata = {
        "word": word,
        "voice_key": voice_key,
        "files": metadata_files,
    }
    with open(output_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    return True, str(output_dir.relative_to(config.PROJECT_ROOT))


# ── Shared State ────────────────────────────────────────────


class _SharedState:
    """Thread-safe container for shared mutable state during parallel processing.

    Tracks results in-memory and flushes to CSV at the end (or periodically)
    to avoid N+1 CSV reads/writes per word.
    """

    CONSECUTIVE_FAILURE_THRESHOLD = 3
    FLUSH_INTERVAL = 50  # flush CSV every N successful words

    def __init__(self, voice_key: str):
        self._lock = threading.Lock()
        self._csv_lock = threading.Lock()
        self.voice_key = voice_key
        self.success_count = 0
        self.failure_count = 0
        self.consecutive_failures = 0
        self.should_stop = False
        self._pending_updates: list[tuple[str, str]] = []  # (word, output_dir)

    def record_success(self, word: str, output_dir: str) -> None:
        with self._lock:
            self.success_count += 1
            self.consecutive_failures = 0
            self._pending_updates.append((word, output_dir))
            should_flush = len(self._pending_updates) >= self.FLUSH_INTERVAL
        if should_flush:
            self.flush_csv()

    def record_failure(self) -> bool:
        """Record a failure. Returns True if threshold hit."""
        with self._lock:
            self.failure_count += 1
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.CONSECUTIVE_FAILURE_THRESHOLD:
                self.should_stop = True
                return True
            return False

    def flush_csv(self) -> None:
        """Flush pending updates to the tracker CSV."""
        with self._csv_lock:
            with self._lock:
                updates = self._pending_updates.copy()
                self._pending_updates.clear()

            if not updates:
                return

            path = config.AUDIO_TRACKER_CSV
            with open(path, "r", newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = list(reader.fieldnames or [])
                rows = list(reader)

            update_map = dict(updates)
            for row in rows:
                if row["word"] in update_map:
                    row[self.voice_key] = update_map[row["word"]]

            _write_tracker_csv(path, fieldnames, rows)


# ── Processing ──────────────────────────────────────────────


def _process_single_word(
    word_info: dict,
    index: int,
    total: int,
    voice_key: str,
    voice_id: str,
    api_key: str,
    client,
    state: _SharedState,
    pbar: tqdm,
) -> None:
    """Process a single word: load data, generate audio, update state."""
    if state.should_stop:
        return

    word = word_info["word"]

    try:
        word_data = load_word_data(word)
    except FileNotFoundError as e:
        print(f"  [{index}/{total}] Skip {word}: {e}")
        pbar.update(1)
        return

    if state.should_stop:
        return

    success, result = generate_word_audio(word_data, voice_key, voice_id, api_key, client)

    if success:
        state.record_success(word, result)
    else:
        print(f"  [{index}/{total}] FAILED {word}: {result}")
        threshold_hit = state.record_failure()
        if threshold_hit:
            print(f"  Stopping: {_SharedState.CONSECUTIVE_FAILURE_THRESHOLD} consecutive failures")

    pbar.update(1)


def run_generation(
    voice_key: str,
    words: list[dict],
    api_key: str,
    parallel: int = 1,
) -> tuple[int, int]:
    """Run audio generation for a list of words.

    Returns (success_count, failure_count).
    """
    voice_config = config.VOICES[voice_key]
    voice_id = voice_config["voice_id"]
    total = len(words)

    state = _SharedState(voice_key)
    pbar = tqdm(total=total, desc=f"  {voice_key}")

    try:
        if parallel <= 1:
            client = create_client()
            try:
                for i, word_info in enumerate(words):
                    if state.should_stop:
                        break
                    _process_single_word(
                        word_info, i + 1, total,
                        voice_key, voice_id, api_key,
                        client, state, pbar,
                    )
            finally:
                client.close()
        else:
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                # Each thread gets its own client for connection reuse
                thread_clients = {}

                def _worker(word_info, index):
                    tid = threading.current_thread().ident
                    if tid not in thread_clients:
                        thread_clients[tid] = create_client()
                    _process_single_word(
                        word_info, index, total,
                        voice_key, voice_id, api_key,
                        thread_clients[tid], state, pbar,
                    )

                futures = {
                    executor.submit(_worker, word_info, i + 1): word_info
                    for i, word_info in enumerate(words)
                }
                try:
                    for future in as_completed(futures):
                        future.result()
                        if state.should_stop:
                            break
                except KeyboardInterrupt:
                    state.should_stop = True
                    raise
                finally:
                    for c in thread_clients.values():
                        c.close()
    except KeyboardInterrupt:
        raise
    finally:
        state.flush_csv()
        pbar.close()

    return state.success_count, state.failure_count


# ── Status ──────────────────────────────────────────────────


def show_status() -> None:
    """Show generation progress for all voices."""
    if not config.AUDIO_TRACKER_CSV.exists():
        print("No tracker CSV found. Run generation to initialize.")
        return

    main_words = _load_main_csv()
    eligible_count = sum(1 for w in main_words.values() if w["output_file"])

    with open(config.AUDIO_TRACKER_CSV, "r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    print(f"Total words in vocabulary: {len(main_words)}")
    print(f"Words with data (eligible): {eligible_count}")
    print()

    for voice_key in _voice_columns():
        if voice_key not in fieldnames:
            print(f"  {voice_key}: not tracked yet")
            continue

        generated = sum(1 for r in rows if r.get(voice_key, "").strip())
        print(f"  {voice_key}: {generated}/{eligible_count} ({generated*100//max(eligible_count,1)}%)")

    print()


# ── CLI ─────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate TTS audio for DailyWord using MiniMax API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_audio.py --status                    # Show progress
  python generate_audio.py --count 10                  # Generate 10 words, default voice
  python generate_audio.py --voice all --count 50      # All voices, 50 words each
  python generate_audio.py --words ability,abandon     # Specific words
  python generate_audio.py --count 50 -f 9,8           # 50 words per frequency tier
  python generate_audio.py --dry-run --count 5         # Preview
        """,
    )

    parser.add_argument(
        "--status", action="store_true",
        help="Show generation progress and exit",
    )
    parser.add_argument(
        "--voice", type=str, default=config.DEFAULT_VOICE,
        help=f"Voice key to use (default: {config.DEFAULT_VOICE}), or 'all' for all voices",
    )
    parser.add_argument(
        "--words", type=str, default=None,
        help="Comma-separated list of specific words to generate",
    )
    parser.add_argument(
        "--count", "-n", type=int, default=None,
        help="Number of ungenerated words to process (default: all eligible)",
    )
    parser.add_argument(
        "--frequencies", "-f", type=str, default=None,
        help="Comma-separated frequency tiers (e.g., 9,8). Loads --count words per tier.",
    )
    parser.add_argument(
        "--parallel", "-j", type=int, default=config.MINIMAX_TTS_PARALLEL_WORKERS,
        help=f"Number of parallel workers (default: {config.MINIMAX_TTS_PARALLEL_WORKERS})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would be generated without calling API",
    )

    args = parser.parse_args()

    # Initialize tracker CSV
    init_tracker_csv()

    if args.status:
        show_status()
        return

    # Parse frequencies
    frequencies = None
    if args.frequencies:
        frequencies = [int(f.strip()) for f in args.frequencies.split(",")]

    # Parse specific words
    specific_words = None
    if args.words:
        specific_words = [w.strip() for w in args.words.split(",")]

    # Determine voices to process
    if args.voice == "all":
        voice_keys = _voice_columns()
    else:
        if args.voice not in config.VOICES:
            print(f"Error: Unknown voice '{args.voice}'. Available: {', '.join(_voice_columns())}")
            sys.exit(1)
        voice_keys = [args.voice]

    # Load main CSV once for all voices
    main_words = _load_main_csv()

    api_key = None
    if not args.dry_run:
        try:
            api_key = get_api_key()
        except MiniMaxTTSError as e:
            print(f"Error: {e}")
            sys.exit(1)

    for voice_key in voice_keys:
        words = load_ungenerated_words(
            voice_key,
            count=args.count,
            frequencies=frequencies,
            specific_words=specific_words,
            main_words=main_words,
        )

        if not words:
            print(f"\n{voice_key}: No ungenerated words to process.")
            continue

        print(f"\n{'=' * 60}")
        print(f"Voice: {voice_key} ({config.VOICES[voice_key]['voice_id']})")
        print(f"Words to process: {len(words)}")
        if frequencies:
            print(f"Frequency filter: {frequencies}")
        if args.dry_run:
            print("Mode: DRY RUN")
            for w in words[:20]:
                print(f"  {w['word']} (freq={w['frequency']})")
            if len(words) > 20:
                print(f"  ... and {len(words) - 20} more")
        print(f"{'=' * 60}")

        if args.dry_run:
            continue

        try:
            success, failed = run_generation(
                voice_key, words, api_key,
                parallel=args.parallel,
            )
            print(f"\n  Done: {success} generated, {failed} failed, {len(words) - success - failed} skipped")
        except KeyboardInterrupt:
            print("\n  Interrupted. Already-generated audio is safe.")
            sys.exit(1)


if __name__ == "__main__":
    main()
