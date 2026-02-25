"""Step 2: Word Enrichment - Add phonetics and POS from dictionary API."""

import asyncio
import json
from pathlib import Path

import httpx
from tenacity import RetryError
from tqdm import tqdm

import config
from src.models import EnrichedWord, SelectedWord
from src.dictionary_client import lookup_word, WordNotFoundError, DictionaryLookupError
from src.checkpoint import CheckpointManager
from src.logger import get_logger


def load_vocabulary_words(path: Path = config.VOCABULARY_TXT) -> list[SelectedWord]:
    """Load words from vocabulary.txt file (one word per line)."""
    words = []
    with open(path, "r") as f:
        for line in f:
            word = line.strip()
            if word:  # Skip empty lines
                words.append(SelectedWord(word=word))
    return words


def load_enriched_words(path: Path = config.ENRICHED_WORDS_JSON) -> list[EnrichedWord]:
    """Load previously enriched words from JSON."""
    if not path.exists():
        return []
    with open(path, "r") as f:
        data = json.load(f)
    return [EnrichedWord(**item) for item in data]


def save_enriched_words(words: list[EnrichedWord], path: Path = config.ENRICHED_WORDS_JSON) -> None:
    """Save enriched words to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump([w.model_dump() for w in words], f, indent=2)


async def enrich_word(
    word: str,
    client: httpx.AsyncClient,
) -> EnrichedWord:
    """
    Enrich a single word with phonetic and POS data.

    Args:
        word: The word to enrich
        client: Async HTTP client

    Returns:
        EnrichedWord with phonetic and POS data
    """
    try:
        result = await lookup_word(word, client)
        return EnrichedWord(
            word=word,
            phonetic=result.get("phonetic"),
            pos=result.get("pos", []),
        )
    except WordNotFoundError:
        return EnrichedWord(word=word, phonetic=None, pos=[])
    except DictionaryLookupError:
        return EnrichedWord(word=word, phonetic=None, pos=[])
    except RetryError:
        # All retries exhausted (likely rate limited)
        return EnrichedWord(word=word, phonetic=None, pos=[])


async def enrich_words_async(
    selected_words: list[SelectedWord],
    checkpoint: CheckpointManager,
    word_range: tuple[int, int] | None = None,
    resume: bool = False,
) -> list[EnrichedWord]:
    """
    Enrich words with dictionary data.

    Args:
        selected_words: List of words to enrich
        checkpoint: Checkpoint manager for progress tracking
        word_range: Optional (start, end) tuple for filtering by row index (0-based, end exclusive)
        resume: Whether to resume from checkpoint

    Returns:
        List of enriched words
    """
    # Load existing results if resuming
    enriched_words = load_enriched_words() if resume else []
    enriched_dict = {w.word: w for w in enriched_words}

    # Filter words by row index range (slice-based)
    if word_range:
        start, end = word_range
        filtered_words = selected_words[start:end]
    else:
        filtered_words = selected_words

    words_to_process = filtered_words

    # Filter out already processed words if resuming
    if resume:
        words_to_process = [
            w for w in words_to_process
            if not checkpoint.is_processed(w.word)
        ]

    if not words_to_process:
        logger = get_logger()
        logger.info("  No words to process (all already completed)")
        return [enriched_dict[sw.word] for sw in filtered_words if sw.word in enriched_dict]

    logger = get_logger()
    logger.info(f"  Processing {len(words_to_process)} words...")

    async with httpx.AsyncClient() as client:
        # Use semaphore for rate limiting (2 concurrent requests to avoid 429)
        semaphore = asyncio.Semaphore(2)

        async def process_with_semaphore(sw: SelectedWord, idx: int):
            async with semaphore:
                result = await enrich_word(sw.word, client)
                enriched_dict[sw.word] = result
                checkpoint.mark_processed(sw.word, idx)
                await asyncio.sleep(0.5)  # Delay between requests to avoid rate limiting
                return result

        # Process with progress bar
        with tqdm(total=len(words_to_process), desc="  Enriching") as pbar:
            async def process_and_update(sw: SelectedWord, idx: int):
                result = await process_with_semaphore(sw, idx)
                pbar.update(1)
                return result

            tasks = [
                process_and_update(sw, i)
                for i, sw in enumerate(words_to_process)
            ]
            await asyncio.gather(*tasks)

    # Rebuild ordered list based on filtered words
    result = []
    for sw in filtered_words:
        if sw.word in enriched_dict:
            result.append(enriched_dict[sw.word])

    return result


def run_step2(
    word_range: tuple[int, int] | None = None,
    resume: bool = False,
    dry_run: bool = False,
) -> list[EnrichedWord]:
    """
    Run Step 2 of the pipeline.

    Args:
        word_range: Optional (start, end) tuple for filtering by row index (0-based, end exclusive)
        resume: Whether to resume from checkpoint
        dry_run: If True, only process a small number of words

    Returns:
        List of enriched words
    """
    logger = get_logger()
    logger.info("Step 2: Enriching words with dictionary data...")

    # Load selected words
    selected_words = load_vocabulary_words()
    logger.info(f"  Loaded {len(selected_words)} selected words")

    # Apply dry run limit (process first N words by row order)
    if dry_run and word_range is None:
        # For dry run, just take first N words
        selected_words = selected_words[:config.DRY_RUN_LIMIT]
        logger.info(f"  Dry run: processing {len(selected_words)} words")

    # Initialize checkpoint
    checkpoint = CheckpointManager(config.STEP2_CHECKPOINT)

    # Run async enrichment
    enriched = asyncio.run(
        enrich_words_async(selected_words, checkpoint, word_range, resume)
    )

    # Save results
    save_enriched_words(enriched)
    logger.info(f"  Saved {len(enriched)} enriched words to: {config.ENRICHED_WORDS_JSON}")

    # Report statistics
    with_phonetic = sum(1 for w in enriched if w.phonetic)
    with_pos = sum(1 for w in enriched if w.pos)
    logger.info(f"  Words with phonetic: {with_phonetic}/{len(enriched)}")
    logger.info(f"  Words with POS: {with_pos}/{len(enriched)}")

    return enriched


if __name__ == "__main__":
    run_step2()
