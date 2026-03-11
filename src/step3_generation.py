"""Step 3: LLM Example Generation - Generate examples using Claude."""

import csv
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

import config
from src.models import EnrichedWord, FinalWordEntry, LLMGenerationResult
from src.claude_client import (
    generate_examples_for_word,
    enrich_examples,
    ClaudeGenerationError,
    ClaudeConsecutiveFailureError,
)
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
        phonetic=enriched.phonetic,
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


def run_step3(
    enriched_words: list[EnrichedWord],
    timestamp: datetime,
    test_mode: bool = False,
    model_short: str = "",
) -> list[FinalWordEntry]:
    """Run Step 3: generate examples for each word, save per-word JSON, update CSV.

    Args:
        enriched_words: Enriched words to process (in-memory)
        timestamp: Timestamp for output file naming
        test_mode: If True, save to test_output/ and do NOT update CSV
        model_short: Short model name (used for test output path)

    Returns:
        List of final word entries
    """
    logger = get_logger()
    logger.info("Step 3: Generating examples with Claude...")

    generation_prompt = load_prompt_template(config.EXAMPLE_GENERATION_PROMPT)
    enrichment_prompt = load_prompt_template(config.EXAMPLE_ENRICHMENT_PROMPT)

    if not enriched_words:
        logger.info("  No words to process")
        return []

    logger.info(f"  Processing {len(enriched_words)} words...")

    validation_warnings = []
    total_words = len(enriched_words)
    consecutive_failures = 0
    CONSECUTIVE_FAILURE_THRESHOLD = 2
    results: list[FinalWordEntry] = []

    try:
        for i, enriched in enumerate(tqdm(enriched_words, desc="  Generating")):
            logger.info(f"  [{i+1}/{total_words}] Processing: {enriched.word}")
            entry, errors = generate_for_word(enriched, generation_prompt, enrichment_prompt)

            if entry:
                # Determine output path
                if test_mode:
                    output_path = config.get_test_output_path(model_short, enriched.word, timestamp)
                else:
                    output_path = config.get_word_output_path(enriched.word, timestamp)

                # Save per-word JSON
                save_word_entry(entry, output_path)
                logger.info(f"  [{i+1}/{total_words}] Saved: {output_path}")

                # Update CSV (unless test mode)
                if not test_mode:
                    relative_path = str(output_path.relative_to(config.PROJECT_ROOT))
                    update_csv_output_file(enriched.word, relative_path)

                results.append(entry)
                consecutive_failures = 0

                if errors:
                    validation_warnings.append((enriched.word, errors))
                    logger.warning(f"  [{i+1}/{total_words}] Validation warning for {enriched.word}: {errors}")
            else:
                logger.error(f"  [{i+1}/{total_words}] Failed: {enriched.word} - {errors}")

                is_cli_error = any("Claude CLI error" in str(e) for e in errors)
                if is_cli_error:
                    consecutive_failures += 1
                    logger.warning(f"  Consecutive Claude CLI failures: {consecutive_failures}/{CONSECUTIVE_FAILURE_THRESHOLD}")
                    if consecutive_failures >= CONSECUTIVE_FAILURE_THRESHOLD:
                        raise ClaudeConsecutiveFailureError(
                            f"Stopping after {CONSECUTIVE_FAILURE_THRESHOLD} consecutive Claude CLI errors"
                        )
                else:
                    consecutive_failures = 0

    except ClaudeConsecutiveFailureError as e:
        logger.error(f"  {e}")
        logger.error("  Stopping early due to consecutive failures. Already-saved words are safe.")

    logger.info(f"  Successfully processed: {len(results)}/{total_words}")

    if validation_warnings:
        logger.warning(f"Validation warnings ({len(validation_warnings)} words):")
        for word, errors in validation_warnings[:5]:
            logger.warning(f"  {word}: {errors}")
        if len(validation_warnings) > 5:
            logger.warning(f"  ... and {len(validation_warnings) - 5} more")

    return results


if __name__ == "__main__":
    from src.step2_enrichment import load_unprocessed_words, run_step2
    words = load_unprocessed_words(config.DRY_RUN_LIMIT)
    enriched = run_step2(words)
    run_step3(enriched, datetime.now(), test_mode=True, model_short=config.model_short_name(config.CLAUDE_MODEL))
