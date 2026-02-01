"""Step 3: LLM Example Generation - Generate examples using Claude."""

import json
from pathlib import Path

from tqdm import tqdm

import config
from src.models import EnrichedWord, FinalWordEntry, LLMGenerationResult
from src.claude_client import (
    generate_examples_for_word,
    ClaudeGenerationError,
)
from src.checkpoint import CheckpointManager
from src.step2_enrichment import load_enriched_words


def load_prompt_template(path: Path = config.EXAMPLE_GENERATION_PROMPT) -> str:
    """Load the prompt template from file."""
    with open(path, "r") as f:
        return f.read()


def load_final_output(path: Path = config.FINAL_OUTPUT_JSON) -> list[FinalWordEntry]:
    """Load previously generated final output from JSON."""
    if not path.exists():
        return []
    with open(path, "r") as f:
        data = json.load(f)
    return [FinalWordEntry(**item) for item in data]


def save_final_output(
    words: list[FinalWordEntry], path: Path = config.FINAL_OUTPUT_JSON
) -> None:
    """Save final output to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump([w.model_dump() for w in words], f, ensure_ascii=False, indent=2)


def create_final_entry(
    enriched: EnrichedWord, generated: LLMGenerationResult
) -> FinalWordEntry:
    """
    Combine enriched word data with LLM-generated content.

    Args:
        enriched: Word with phonetic and POS data
        generated: LLM-generated definition and examples

    Returns:
        Complete FinalWordEntry
    """
    return FinalWordEntry(
        word=enriched.word,
        phonetic=enriched.phonetic,
        pos=enriched.pos,
        selected_pos=generated.selected_pos,
        definition=generated.definition,
        examples=generated.examples,
    )


def validate_entry(entry: FinalWordEntry) -> list[str]:
    """
    Validate a final word entry.

    Args:
        entry: The entry to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Check we have all 11 styles
    if len(entry.examples) != 11:
        errors.append(f"Expected 11 examples, got {len(entry.examples)}")

    # Check each example
    for i, ex in enumerate(entry.examples):
        # Check translated_word is in translation
        if ex.translated_word and ex.translated_word not in ex.translation:
            errors.append(
                f"Example {i+1}: translated_word '{ex.translated_word}' not in translation"
            )

        # Check for em dash
        if "—" in ex.sentence or "—" in ex.translation:
            errors.append(f"Example {i+1}: contains em dash")

    return errors


def generate_for_word(
    enriched: EnrichedWord,
    prompt_template: str,
) -> tuple[FinalWordEntry | None, list[str]]:
    """
    Generate examples for a single word.

    Args:
        enriched: Enriched word data
        prompt_template: The prompt template to use

    Returns:
        Tuple of (FinalWordEntry or None, list of errors)
    """
    try:
        result = generate_examples_for_word(
            word=enriched.word,
            pos=enriched.pos,
            prompt_template=prompt_template,
        )
        entry = create_final_entry(enriched, result)
        errors = validate_entry(entry)
        return entry, errors
    except ClaudeGenerationError as e:
        return None, [str(e)]


def run_step3(
    resume: bool = False,
    dry_run: bool = False,
) -> list[FinalWordEntry]:
    """
    Run Step 3 of the pipeline.

    Args:
        resume: Whether to resume from checkpoint
        dry_run: If True, only process a small number of words

    Returns:
        List of final word entries
    """
    print("Step 3: Generating examples with Claude...")

    # Load enriched words
    enriched_words = load_enriched_words()
    print(f"  Loaded {len(enriched_words)} enriched words")

    # Load prompt template
    prompt_template = load_prompt_template()

    # Initialize checkpoint
    checkpoint = CheckpointManager(config.STEP3_CHECKPOINT)

    # Load existing results if resuming
    final_entries = load_final_output() if resume else []
    entries_dict = {e.word: e for e in final_entries}

    # Apply dry run limit
    if dry_run:
        words_to_process = enriched_words[:config.DRY_RUN_LIMIT]
        print(f"  Dry run: processing {len(words_to_process)} words")
    else:
        words_to_process = enriched_words

    # Filter out already processed words if resuming
    if resume:
        words_to_process = [
            w for w in words_to_process if not checkpoint.is_processed(w.word)
        ]

    if not words_to_process:
        print("  No words to process (all already completed)")
        return final_entries

    print(f"  Processing {len(words_to_process)} words...")

    # Process each word
    validation_warnings = []

    for i, enriched in enumerate(tqdm(words_to_process, desc="  Generating")):
        entry, errors = generate_for_word(enriched, prompt_template)

        if entry:
            entries_dict[enriched.word] = entry
            checkpoint.mark_processed(enriched.word, i)

            if errors:
                validation_warnings.append((enriched.word, errors))
        else:
            checkpoint.mark_failed(enriched.word)
            print(f"\n  Failed: {enriched.word} - {errors}")

        # Save periodically (every 10 words)
        if (i + 1) % 10 == 0:
            result = list(entries_dict.values())
            save_final_output(result)

    # Final save
    result = list(entries_dict.values())
    save_final_output(result)

    print(f"  Saved {len(result)} entries to: {config.FINAL_OUTPUT_JSON}")
    print(f"  Successfully processed: {checkpoint.processed_count}")
    print(f"  Failed: {checkpoint.failed_count}")

    if validation_warnings:
        print(f"\n  Validation warnings ({len(validation_warnings)} words):")
        for word, errors in validation_warnings[:5]:
            print(f"    {word}: {errors}")
        if len(validation_warnings) > 5:
            print(f"    ... and {len(validation_warnings) - 5} more")

    return result


if __name__ == "__main__":
    run_step3(dry_run=True)
