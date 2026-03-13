"""Step 3: LLM Example Generation - Generate examples using Claude."""

import csv
import json
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

import config
from src.models import EnrichedWord, FinalWordEntry, LLMGenerationResult, SelectedWord
from src.claude_client import (
    generate_examples_for_word,
    enrich_examples,
    ClaudeGenerationError,
    ClaudeConsecutiveFailureError,
)
from src.step2_enrichment import enrich_single_word
from src.logger import get_logger


def load_prompt_template(path: Path = config.EXAMPLE_GENERATION_PROMPT) -> str:
    """Load the prompt template from file."""
    with open(path, "r") as f:
        return f.read()


def save_word_entry(entry: FinalWordEntry, output_path: Path) -> None:
    """Save a single word entry as a JSON object to the given path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(entry.model_dump(), f, ensure_ascii=False, indent=2)


def update_csv_output_file(word: str, relative_path: str, csv_path: Path = config.VOCABULARY_CSV) -> None:
    """Update the output_file column in CSV for one word. Uses atomic write."""
    # Read all rows
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Update the matching row
    for row in rows:
        if row["word"].strip() == word:
            row["output_file"] = relative_path
            break

    # Atomic write: write to temp file, then replace
    fd, tmp_path = tempfile.mkstemp(dir=csv_path.parent, suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, csv_path)
    except Exception:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def create_final_entry(
    enriched: EnrichedWord, generated: LLMGenerationResult
) -> FinalWordEntry:
    """Combine enriched word data with LLM-generated content."""
    return FinalWordEntry(
        word=enriched.word,
        phonetic=generated.phonetic or enriched.phonetic,  # Prefer LLM's POS-aware phonetic
        pos=enriched.pos,
        selected_pos=generated.selected_pos,
        definition=generated.definition,
        examples=generated.examples,
    )


def validate_entry(entry: FinalWordEntry) -> list[str]:
    """Validate a final word entry. Returns list of error messages (empty if valid)."""
    errors = []

    if len(entry.examples) != config.EXAMPLES_PER_WORD:
        errors.append(f"Expected {config.EXAMPLES_PER_WORD} examples, got {len(entry.examples)}")

    for i, ex in enumerate(entry.examples):
        if ex.translated_word and ex.translated_word not in ex.translation:
            errors.append(
                f"Example {i+1}: translated_word '{ex.translated_word}' not in translation"
            )
        if "\u2014" in ex.sentence or "\u2014" in ex.translation:
            errors.append(f"Example {i+1}: contains em dash")

    selected_count = sum(1 for ex in entry.examples if ex.display_order is not None)
    if selected_count != 4:
        errors.append(f"Expected 4 selected examples, got {selected_count}")

    display_orders = sorted([ex.display_order for ex in entry.examples if ex.display_order is not None])
    if display_orders != [1, 2, 3, 4]:
        errors.append(f"Invalid display_order values: {display_orders}")

    return errors


def generate_for_word(
    enriched: EnrichedWord,
    generation_prompt: str,
    enrichment_prompt: str,
) -> tuple[FinalWordEntry | None, list[str]]:
    """Generate examples for a single word, enrich with translated_word and display_order."""
    try:
        result = generate_examples_for_word(
            word=enriched.word,
            pos=enriched.pos,
            prompt_template=generation_prompt,
        )

        enrichments = enrich_examples(
            word=enriched.word,
            selected_pos=result.selected_pos,
            examples=result.examples,
            prompt_template=enrichment_prompt,
        )
        for i, enrichment in enumerate(enrichments):
            result.examples[i].translated_word = enrichment["translated_word"]
            result.examples[i].display_order = enrichment["display_order"]

        entry = create_final_entry(enriched, result)
        errors = validate_entry(entry)
        return entry, errors
    except ClaudeGenerationError as e:
        return None, [str(e)]


class _SharedState:
    """Thread-safe container for shared mutable state during parallel processing."""

    CONSECUTIVE_FAILURE_THRESHOLD = 2

    def __init__(self):
        self._lock = threading.Lock()
        self._csv_lock = threading.Lock()
        self.results: list[FinalWordEntry] = []
        self.validation_warnings: list[tuple[str, list[str]]] = []
        self.consecutive_failures = 0
        self.should_stop = False

    def record_success(self, entry: FinalWordEntry, errors: list[str]) -> None:
        with self._lock:
            self.results.append(entry)
            self.consecutive_failures = 0
            if errors:
                self.validation_warnings.append((entry.word, errors))

    def record_failure(self, is_cli_error: bool) -> bool:
        """Record a failure. Returns True if the consecutive failure threshold is hit."""
        with self._lock:
            if is_cli_error:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.CONSECUTIVE_FAILURE_THRESHOLD:
                    self.should_stop = True
                    return True
            else:
                self.consecutive_failures = 0
            return False

    def update_csv(self, word: str, relative_path: str) -> None:
        with self._csv_lock:
            update_csv_output_file(word, relative_path)


def _process_single_word(
    selected: SelectedWord,
    index: int,
    total_words: int,
    generation_prompt: str,
    enrichment_prompt: str,
    timestamp: datetime,
    test_mode: bool,
    model_short: str,
    state: _SharedState,
    pbar: tqdm,
) -> None:
    """Process a single word: enrich, generate, save, update CSV."""
    logger = get_logger()

    if state.should_stop:
        return

    logger.info(f"  [{index}/{total_words}] Enriching: {selected.word}")
    enriched = enrich_single_word(selected.word)
    logger.info(f"  [{index}/{total_words}] Enriched: {selected.word} "
                f"(phonetic={enriched.phonetic}, pos={enriched.pos})")

    if state.should_stop:
        return

    logger.info(f"  [{index}/{total_words}] Generating: {selected.word}")
    max_attempts = 3
    entry = None
    errors = []
    for attempt in range(1, max_attempts + 1):
        entry, errors = generate_for_word(enriched, generation_prompt, enrichment_prompt)
        if entry:
            break
        if attempt < max_attempts:
            logger.warning(f"  [{index}/{total_words}] Attempt {attempt}/{max_attempts} failed for {selected.word}, retrying...")

    if entry:
        # Determine output path
        if test_mode:
            output_path = config.get_test_output_path(model_short, selected.word, timestamp)
        else:
            output_path = config.get_word_output_path(selected.word, timestamp)

        # Save per-word JSON
        save_word_entry(entry, output_path)
        logger.info(f"  [{index}/{total_words}] Saved: {output_path}")

        # Update CSV (unless test mode)
        if not test_mode:
            relative_path = str(output_path.relative_to(config.PROJECT_ROOT))
            state.update_csv(selected.word, relative_path)

        state.record_success(entry, errors)

        if errors:
            logger.warning(f"  [{index}/{total_words}] Validation warning for {selected.word}: {errors}")
    else:
        logger.error(f"  [{index}/{total_words}] Failed: {selected.word} - {errors}")

        is_cli_error = any("Claude CLI error" in str(e) for e in errors)
        threshold_hit = state.record_failure(is_cli_error)
        if is_cli_error:
            logger.warning(f"  Consecutive Claude CLI failures: {state.consecutive_failures}/{_SharedState.CONSECUTIVE_FAILURE_THRESHOLD}")
        if threshold_hit:
            logger.error(f"  Stopping after {_SharedState.CONSECUTIVE_FAILURE_THRESHOLD} consecutive Claude CLI errors")

    pbar.update(1)


def run_step3(
    words: list[SelectedWord],
    timestamp: datetime,
    test_mode: bool = False,
    model_short: str = "",
    parallel: int = 1,
) -> list[FinalWordEntry]:
    """Run Step 3: enrich and generate examples for each word, save per-word JSON, update CSV.

    Args:
        words: Words to process (enrichment happens per-word inline)
        timestamp: Timestamp for output file naming
        test_mode: If True, save to test_output/ and do NOT update CSV
        model_short: Short model name (used for test output path)
        parallel: Number of parallel workers (1 = serial)

    Returns:
        List of final word entries
    """
    logger = get_logger()
    logger.info("Processing words (enrich + generate)...")

    generation_prompt = load_prompt_template(config.EXAMPLE_GENERATION_PROMPT)
    enrichment_prompt = load_prompt_template(config.EXAMPLE_ENRICHMENT_PROMPT)

    if not words:
        logger.info("  No words to process")
        return []

    total_words = len(words)
    logger.info(f"  Processing {total_words} words (parallel={parallel})...")

    state = _SharedState()
    pbar = tqdm(total=total_words, desc="  Processing")

    try:
        if parallel <= 1:
            # Serial processing
            for i, selected in enumerate(words):
                if state.should_stop:
                    break
                _process_single_word(
                    selected, i + 1, total_words,
                    generation_prompt, enrichment_prompt,
                    timestamp, test_mode, model_short,
                    state, pbar,
                )
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                futures = {
                    executor.submit(
                        _process_single_word,
                        selected, i + 1, total_words,
                        generation_prompt, enrichment_prompt,
                        timestamp, test_mode, model_short,
                        state, pbar,
                    ): selected
                    for i, selected in enumerate(words)
                }
                try:
                    for future in as_completed(futures):
                        future.result()  # Propagate exceptions
                        if state.should_stop:
                            break
                except KeyboardInterrupt:
                    logger.warning("  Interrupt received, waiting for in-progress workers to finish...")
                    state.should_stop = True
                    # Let ThreadPoolExecutor.__exit__ wait for running futures
                    raise

    except KeyboardInterrupt:
        raise
    finally:
        pbar.close()

    logger.info(f"  Successfully processed: {len(state.results)}/{total_words}")

    if state.validation_warnings:
        logger.warning(f"Validation warnings ({len(state.validation_warnings)} words):")
        for word, errors in state.validation_warnings[:5]:
            logger.warning(f"  {word}: {errors}")
        if len(state.validation_warnings) > 5:
            logger.warning(f"  ... and {len(state.validation_warnings) - 5} more")

    return state.results


if __name__ == "__main__":
    from src.step2_enrichment import load_unprocessed_words
    words = load_unprocessed_words(config.DRY_RUN_LIMIT)
    run_step3(words, datetime.now(), test_mode=True, model_short=config.model_short_name(config.CLAUDE_MODEL))
