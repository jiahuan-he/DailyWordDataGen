"""Free Dictionary API client for word lookups."""

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import config


class DictionaryLookupError(Exception):
    """Raised when dictionary lookup fails."""

    pass


class WordNotFoundError(DictionaryLookupError):
    """Raised when word is not found in dictionary."""

    pass


class RateLimitError(DictionaryLookupError):
    """Raised when rate limited by the API."""

    pass


def is_rate_limit_error(exception: BaseException) -> bool:
    """Check if exception is a rate limit error (429 or 503)."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in (429, 503)
    return False


def parse_dictionary_response(data: list) -> dict:
    """
    Parse the Free Dictionary API response.

    Args:
        data: Raw API response (list of entries)

    Returns:
        Dictionary with phonetic and pos fields
    """
    phonetic = None
    pos_set = set()

    for entry in data:
        # Extract phonetic (prefer one with text)
        if not phonetic:
            # Try phonetic field first
            if entry.get("phonetic"):
                phonetic = entry["phonetic"]
            # Then try phonetics array
            elif entry.get("phonetics"):
                for p in entry["phonetics"]:
                    if p.get("text"):
                        phonetic = p["text"]
                        break

        # Extract parts of speech from meanings
        for meaning in entry.get("meanings", []):
            if meaning.get("partOfSpeech"):
                pos_set.add(meaning["partOfSpeech"])

    return {
        "phonetic": phonetic,
        "pos": sorted(list(pos_set)),
    }


@retry(
    stop=stop_after_attempt(config.DICTIONARY_API_MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
)
def lookup_word_sync(word: str) -> dict:
    """
    Look up a word in the Free Dictionary API (synchronous).

    Args:
        word: The word to look up

    Returns:
        Dictionary containing phonetic and parts of speech

    Raises:
        WordNotFoundError: If word is not in the dictionary
        DictionaryLookupError: If lookup fails for other reasons
    """
    url = f"{config.FREE_DICTIONARY_API_URL}/{word}"

    with httpx.Client() as client:
        response = client.get(url, timeout=config.DICTIONARY_API_TIMEOUT)

    if response.status_code == 404:
        raise WordNotFoundError(f"Word not found: {word}")

    response.raise_for_status()
    data = response.json()

    if not data or not isinstance(data, list):
        raise DictionaryLookupError(f"Unexpected response format for: {word}")

    return parse_dictionary_response(data)
