"""Step 2: Word Enrichment - Add phonetics and POS from dictionary API."""

import asyncio
import csv
from pathlib import Path

import httpx
from tenacity import RetryError
from tqdm import tqdm

import config
from src.models import EnrichedWord, SelectedWord
from src.dictionary_client import lookup_word, WordNotFoundError, DictionaryLookupError
from src.logger import get_logger


def load_vocabulary_words(path: Path = config.VOCABULARY_CSV) -> list[SelectedWord]:
    """Load words from CSV with word, frequency, output_file columns."""
    words = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = row["word"].strip()
            if word:
                words.append(SelectedWord(
                    word=word,
                    frequency=int(float(row.get("frequency", 0) or 0)),
                    output_file=row.get("output_file", "").strip(),
                ))
    return words


def load_unprocessed_words(count: int, path: Path = config.VOCABULARY_CSV) -> list[SelectedWord]:
    """Return the first `count` words from CSV where output_file is empty."""
    all_words = load_vocabulary_words(path)
    unprocessed = [w for w in all_words if not w.output_file]
    return unprocessed[:count]


async def enrich_word(
    word: str,
    client: httpx.AsyncClient,
) -> EnrichedWord:
    """Enrich a single word with phonetic and POS data."""
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
        return EnrichedWord(word=word, phonetic=None, pos=[])


async def enrich_words_async(
    selected_words: list[SelectedWord],
) -> list[EnrichedWord]:
    """Enrich words with dictionary data (in-memory, no file I/O)."""
    if not selected_words:
        logger = get_logger()
        logger.info("  No words to process (all already completed)")
        return []

    logger = get_logger()
    logger.info(f"  Processing {len(selected_words)} words...")

    enriched_dict: dict[str, EnrichedWord] = {}

    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(2)

        async def process_with_semaphore(sw: SelectedWord):
            async with semaphore:
                result = await enrich_word(sw.word, client)
                enriched_dict[sw.word] = result
                await asyncio.sleep(0.5)
                return result

        with tqdm(total=len(selected_words), desc="  Enriching") as pbar:
            async def process_and_update(sw: SelectedWord):
                result = await process_with_semaphore(sw)
                pbar.update(1)
                return result

            tasks = [process_and_update(sw) for sw in selected_words]
            await asyncio.gather(*tasks)

    # Rebuild ordered list
    return [enriched_dict[sw.word] for sw in selected_words if sw.word in enriched_dict]


def run_step2(selected_words: list[SelectedWord]) -> list[EnrichedWord]:
    """Run Step 2: enrich the given words with dictionary data (in-memory).

    Args:
        selected_words: Words to enrich

    Returns:
        List of enriched words
    """
    logger = get_logger()
    logger.info("Step 2: Enriching words with dictionary data...")
    logger.info(f"  {len(selected_words)} words to enrich")

    enriched = asyncio.run(enrich_words_async(selected_words))

    # Report statistics
    with_phonetic = sum(1 for w in enriched if w.phonetic)
    with_pos = sum(1 for w in enriched if w.pos)
    logger.info(f"  Words with phonetic: {with_phonetic}/{len(enriched)}")
    logger.info(f"  Words with POS: {with_pos}/{len(enriched)}")

    return enriched


if __name__ == "__main__":
    words = load_unprocessed_words(10)
    run_step2(words)
