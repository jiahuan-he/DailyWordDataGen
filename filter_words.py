"""Filter vocabulary words for suitability in the DailyWord learning app.

Stage 1: Deterministic filters (British/American spelling duplicates)
Stage 2: LLM-based judgment via Claude headless mode
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

import config
from src.claude_client import (
    ClaudeGenerationError,
    ClaudeParseError,
    ClaudeTimeoutError,
    extract_json_from_response,
)
from src.models import WordFilterResult
from src.step2_enrichment import load_vocabulary_words

# ── Paths ────────────────────────────────────────────────────

FILTER_RESULTS_PATH = config.PROJECT_ROOT / "source" / "word_filter_results.json"
EXCLUDED_WORDS_PATH = config.PROJECT_ROOT / "source" / "excluded_words.json"
FILTER_PROMPT_PATH = config.PROMPTS_DIR / "word_filter.txt"

# ── British → American spelling pairs ────────────────────────
# Only the British variant is removed (if BOTH exist in the CSV).

BRITISH_AMERICAN_PAIRS = {
    # -ise / -ize
    "epitomise": "epitomize",
    "homogenise": "homogenize",
    "idolise": "idolize",
    "jeopardise": "jeopardize",
    "legalise": "legalize",
    "mesmerise": "mesmerize",
    "mobilise": "mobilize",
    "monopolise": "monopolize",
    "neutralise": "neutralize",
    "patronise": "patronize",
    "scrutinise": "scrutinize",
    "subsidise": "subsidize",
    "synthesise": "synthesize",
    "tantalise": "tantalize",
    # -our / -or
    "ardour": "ardor",
    "armour": "armor",
    "armoury": "armory",
    "endeavour": "endeavor",
    "fervour": "fervor",
    "flavouring": "flavoring",
    "glamour": "glamor",
    "glamourous": "glamorous",
    "odour": "odor",
    "rancour": "rancor",
    "savour": "savor",
    "succour": "succor",
    "valour": "valor",
    # -re / -er
    "calibre": "caliber",
    "meagre": "meager",
    "sombre": "somber",
    "spectre": "specter",
}

# ── Default settings ─────────────────────────────────────────

DEFAULT_BATCH_SIZE = 20
DEFAULT_PARALLEL_WORKERS = 10
DEFAULT_MODEL = "claude-sonnet-4-6"
CLAUDE_TIMEOUT = 120  # seconds per call


# ── Load / Save ──────────────────────────────────────────────


def load_filter_results() -> dict:
    """Load existing filter results from JSON file."""
    if FILTER_RESULTS_PATH.exists():
        with open(FILTER_RESULTS_PATH, "r") as f:
            return json.load(f)
    return {"metadata": {}, "results": {}}


def save_filter_results(data: dict) -> None:
    """Atomically save filter results to JSON file."""
    import os
    import tempfile

    FILTER_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=FILTER_RESULTS_PATH.parent, suffix=".json"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, FILTER_RESULTS_PATH)
    except BaseException:
        os.unlink(tmp_path)
        raise


def update_metadata(data: dict, total_words: int, model: str) -> None:
    """Update the metadata section of filter results."""
    results = data.get("results", {})
    now = datetime.now().isoformat(timespec="seconds")

    kept = sum(1 for r in results.values() if r["verdict"] == "keep")
    removed_det = sum(
        1
        for r in results.values()
        if r["verdict"] == "remove" and r["source"] == "deterministic"
    )
    removed_llm = sum(
        1
        for r in results.values()
        if r["verdict"] == "remove" and r["source"] == "llm"
    )

    data["metadata"] = {
        "created": data.get("metadata", {}).get("created", now),
        "last_updated": now,
        "model": model,
        "total_words": total_words,
        "judged": len(results),
        "kept": kept,
        "removed_deterministic": removed_det,
        "removed_llm": removed_llm,
    }


# ── Stage 1: Deterministic filters ──────────────────────────


def run_deterministic_filters(
    all_word_set: set[str], data: dict
) -> int:
    """Apply deterministic filters. Returns count of newly filtered words."""
    results = data["results"]
    count = 0

    for british, american in BRITISH_AMERICAN_PAIRS.items():
        if british in results:
            continue  # Already judged
        if british in all_word_set and american in all_word_set:
            results[british] = {
                "verdict": "remove",
                "source": "deterministic",
                "reason": "duplicate_british_spelling",
                "kept_variant": american,
            }
            count += 1

    return count


# ── Stage 2: LLM-based judgment ──────────────────────────────


def load_prompt_template() -> str:
    """Load the word filter prompt template."""
    with open(FILTER_PROMPT_PATH, "r") as f:
        return f.read()


def judge_batch(
    words: list[str], prompt_template: str, model: str
) -> dict[str, dict]:
    """Call Claude to judge a batch of words. Returns word -> result dict."""
    import subprocess

    words_json = json.dumps(words, ensure_ascii=False)
    prompt = prompt_template.format(words_json=words_json)

    try:
        result = subprocess.run(
            [
                "claude",
                "-p",
                prompt,
                "--model",
                model,
                "--output-format",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=CLAUDE_TIMEOUT,
            start_new_session=True,
        )
    except subprocess.TimeoutExpired:
        raise ClaudeTimeoutError(f"Claude timed out after {CLAUDE_TIMEOUT}s")
    except FileNotFoundError:
        raise ClaudeGenerationError("Claude CLI not found.")

    if result.returncode != 0:
        error_detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
        raise ClaudeGenerationError(f"Claude CLI error: {error_detail}")

    # Parse JSON output
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ClaudeParseError(f"Failed to parse response: {e}")

    # Extract content from wrapper
    if isinstance(response, dict) and "result" in response:
        content = response["result"]
    elif isinstance(response, dict) and "content" in response:
        content = response["content"]
    else:
        content = result.stdout

    parsed = extract_json_from_response(content)

    if "results" not in parsed:
        raise ClaudeParseError(f"Response missing 'results' field: {parsed}")

    # Convert to word -> result dict
    batch_results = {}
    for item in parsed["results"]:
        word = item["word"]
        entry = {
            "verdict": item["verdict"],
            "source": "llm",
            "reason": item.get("reason", ""),
        }
        if item["verdict"] == "remove" and "category" in item:
            entry["category"] = item["category"]
        batch_results[word] = entry

    return batch_results


def run_llm_filter(
    all_words: list[str],
    data: dict,
    model: str,
    batch_size: int,
    max_count: int | None,
    parallel_workers: int,
) -> int:
    """Run LLM-based filtering. Returns count of newly judged words."""
    results = data["results"]
    prompt_template = load_prompt_template()

    # Collect un-judged words
    unjudged = [w for w in all_words if w not in results]
    if max_count is not None:
        unjudged = unjudged[:max_count]

    if not unjudged:
        print("All words have been judged.")
        return 0

    # Build batches
    batches = [
        unjudged[i : i + batch_size]
        for i in range(0, len(unjudged), batch_size)
    ]

    print(f"Judging {len(unjudged)} words in {len(batches)} batches (batch_size={batch_size}, workers={parallel_workers})")

    judged = 0
    failed_batches = 0
    lock = __import__("threading").Lock()

    def process_batch(batch: list[str]) -> dict[str, dict] | None:
        try:
            return judge_batch(batch, prompt_template, model)
        except (ClaudeGenerationError, ClaudeParseError) as e:
            return None, batch, str(e)

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {
            executor.submit(process_batch, batch): batch
            for batch in batches
        }

        with tqdm(total=len(unjudged), desc="Filtering") as pbar:
            for future in as_completed(futures):
                batch = futures[future]
                result = future.result()

                if isinstance(result, tuple):
                    # Error case
                    _, failed_batch, error_msg = result
                    failed_batches += 1
                    tqdm.write(f"  Batch failed ({failed_batch[0]}...): {error_msg}")
                    pbar.update(len(batch))
                    continue

                # Success - merge results
                with lock:
                    for word, entry in result.items():
                        results[word] = entry
                        judged += 1

                    # Save after each batch for crash resilience
                    update_metadata(data, data["metadata"].get("total_words", 0), model)
                    save_filter_results(data)

                pbar.update(len(batch))

    if failed_batches > 0:
        print(f"\n{failed_batches} batches failed. Re-run to retry.")

    return judged


# ── Export ───────────────────────────────────────────────────


def export_exclusion_list(data: dict) -> None:
    """Export a simple list of excluded words."""
    results = data.get("results", {})
    excluded = sorted(
        word for word, r in results.items() if r["verdict"] == "remove"
    )

    with open(EXCLUDED_WORDS_PATH, "w") as f:
        json.dump(excluded, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(excluded)} excluded words to {EXCLUDED_WORDS_PATH}")


def load_excluded_words() -> set[str]:
    """Load the excluded words set. Used by other pipeline scripts."""
    if EXCLUDED_WORDS_PATH.exists():
        with open(EXCLUDED_WORDS_PATH, "r") as f:
            return set(json.load(f))
    return set()


# ── Stats ────────────────────────────────────────────────────


def print_stats(data: dict, all_words: list[str]) -> None:
    """Print filtering statistics."""
    results = data.get("results", {})
    meta = data.get("metadata", {})

    total = len(all_words)
    judged = len(results)
    remaining = total - judged

    kept = sum(1 for r in results.values() if r["verdict"] == "keep")
    removed = sum(1 for r in results.values() if r["verdict"] == "remove")
    removed_det = sum(
        1
        for r in results.values()
        if r["verdict"] == "remove" and r["source"] == "deterministic"
    )
    removed_llm = sum(
        1
        for r in results.values()
        if r["verdict"] == "remove" and r["source"] == "llm"
    )

    print(f"Total words in CSV:      {total}")
    print(f"Judged:                  {judged}")
    print(f"Remaining:               {remaining}")
    print(f"Kept:                    {kept}")
    print(f"Removed (total):         {removed}")
    print(f"  - Deterministic:       {removed_det}")
    print(f"  - LLM:                 {removed_llm}")

    if removed > 0:
        pct = removed / judged * 100 if judged > 0 else 0
        print(f"Removal rate:            {pct:.1f}%")

    # Category breakdown for LLM removals
    categories: dict[str, int] = {}
    for r in results.values():
        if r["verdict"] == "remove" and r["source"] == "llm":
            cat = r.get("category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1

    if categories:
        print("\nRemoval categories (LLM):")
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")

    # Show some removed words as examples
    removed_words = [
        (w, r) for w, r in results.items() if r["verdict"] == "remove"
    ]
    if removed_words:
        print(f"\nSample removed words (up to 20):")
        for word, r in removed_words[:20]:
            src = r["source"]
            reason = r["reason"]
            cat = r.get("category", "")
            cat_str = f" [{cat}]" if cat else ""
            print(f"  {word}: {reason}{cat_str} ({src})")


# ── CLI ──────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter vocabulary words for suitability in the DailyWord app.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python filter_words.py                    # Judge all remaining words
  python filter_words.py --count 200        # Judge next 200 words
  python filter_words.py --dry-run          # Preview what would be judged
  python filter_words.py --stats            # Show filtering statistics
  python filter_words.py --export           # Export excluded_words.json
        """,
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Max number of words to judge in this run (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Words per Claude call (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL,
        help=f"Claude model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_PARALLEL_WORKERS,
        help=f"Parallel workers (default: {DEFAULT_PARALLEL_WORKERS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be judged without calling Claude",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show current filtering statistics",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export source/excluded_words.json for pipeline integration",
    )

    args = parser.parse_args()

    # Load all words from CSV
    all_word_objs = load_vocabulary_words(config.VOCABULARY_CSV)
    all_words = [w.word for w in all_word_objs]
    all_word_set = set(all_words)

    # Load existing results
    data = load_filter_results()

    if args.stats:
        print_stats(data, all_words)
        return

    if args.export:
        export_exclusion_list(data)
        return

    # Stage 1: Deterministic filters
    det_count = run_deterministic_filters(all_word_set, data)
    if det_count > 0:
        update_metadata(data, len(all_words), args.model)
        save_filter_results(data)
        print(f"Stage 1: Marked {det_count} British spelling duplicates for removal")

    if args.dry_run:
        unjudged = [w for w in all_words if w not in data["results"]]
        count = args.count if args.count else len(unjudged)
        unjudged = unjudged[:count]
        print(f"\nDry run: {len(unjudged)} words would be sent to LLM for judgment")
        if unjudged:
            print(f"First 30: {', '.join(unjudged[:30])}")
        print(f"\nAlready judged: {len(data['results'])} / {len(all_words)}")
        return

    # Stage 2: LLM filter
    llm_count = run_llm_filter(
        all_words,
        data,
        model=args.model,
        batch_size=args.batch_size,
        max_count=args.count,
        parallel_workers=args.workers,
    )

    # Final save with updated metadata
    update_metadata(data, len(all_words), args.model)
    save_filter_results(data)

    print(f"\nDone. Newly judged: {llm_count}")
    print_stats(data, all_words)


if __name__ == "__main__":
    main()
