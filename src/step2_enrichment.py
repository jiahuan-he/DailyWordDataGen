"""Step 2: Word Enrichment - Add phonetics and POS from dictionary API."""

import csv
from pathlib import Path

from tenacity import RetryError

import config
from src.models import EnrichedWord, SelectedWord
from src.dictionary_client import lookup_word_sync, WordNotFoundError, DictionaryLookupError
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


def load_unprocessed_words(
    count: int,
    path: Path = config.VOCABULARY_CSV,
    frequencies: list[int] | None = None,
) -> list[SelectedWord]:
    """Return the first `count` words from CSV where output_file is empty.

    Args:
        count: Max number of words to return (per frequency if frequencies is set).
        path: Path to the vocabulary CSV.
        frequencies: If provided, collect first `count` unprocessed words for each
            frequency tier, then combine preserving CSV order.
    """
    all_words = load_vocabulary_words(path)
    unprocessed = [w for w in all_words if not w.output_file]

    if frequencies is None:
        return unprocessed[:count]

    # Collect up to `count` words per frequency
    freq_counts: dict[int, int] = {f: 0 for f in frequencies}
    selected_set: set[int] = set()
    for i, w in enumerate(unprocessed):
        if w.frequency in freq_counts and freq_counts[w.frequency] < count:
            selected_set.add(i)
            freq_counts[w.frequency] += 1

    # Preserve CSV order
    return [unprocessed[i] for i in sorted(selected_set)]


def enrich_single_word(word: str) -> EnrichedWord:
    """Enrich a single word with phonetic and POS data (synchronous).

    Args:
        word: The word to look up

    Returns:
        EnrichedWord with phonetic/pos data, or empty fields on error
    """
    try:
        result = lookup_word_sync(word)
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
