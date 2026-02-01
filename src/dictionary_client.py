"""Free Dictionary API client for word lookups."""

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config


class DictionaryLookupError(Exception):
    """Raised when dictionary lookup fails."""

    pass


class WordNotFoundError(DictionaryLookupError):
    """Raised when word is not found in dictionary."""

    pass


@retry(
    stop=stop_after_attempt(config.DICTIONARY_API_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
)
async def lookup_word(word: str, client: httpx.AsyncClient) -> dict:
    """
    Look up a word in the Free Dictionary API.

    Args:
        word: The word to look up
        client: Async HTTP client

    Returns:
        Dictionary containing phonetic and parts of speech

    Raises:
        WordNotFoundError: If word is not in the dictionary
        DictionaryLookupError: If lookup fails for other reasons
    """
    url = f"{config.FREE_DICTIONARY_API_URL}/{word}"

    response = await client.get(url, timeout=config.DICTIONARY_API_TIMEOUT)

    if response.status_code == 404:
        raise WordNotFoundError(f"Word not found: {word}")

    response.raise_for_status()
    data = response.json()

    if not data or not isinstance(data, list):
        raise DictionaryLookupError(f"Unexpected response format for: {word}")

    return parse_dictionary_response(data)


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


async def lookup_words_batch(
    words: list[str], batch_size: int = 10
) -> dict[str, dict]:
    """
    Look up multiple words with rate limiting.

    Args:
        words: List of words to look up
        batch_size: Number of concurrent requests

    Returns:
        Dictionary mapping words to their lookup results
    """
    import asyncio

    results = {}
    semaphore = asyncio.Semaphore(batch_size)

    async def lookup_with_semaphore(word: str, client: httpx.AsyncClient):
        async with semaphore:
            try:
                result = await lookup_word(word, client)
                results[word] = result
            except WordNotFoundError:
                results[word] = {"phonetic": None, "pos": [], "error": "not_found"}
            except DictionaryLookupError as e:
                results[word] = {"phonetic": None, "pos": [], "error": str(e)}

    async with httpx.AsyncClient() as client:
        tasks = [lookup_with_semaphore(word, client) for word in words]
        await asyncio.gather(*tasks)

    return results
