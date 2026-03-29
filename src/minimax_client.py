"""MiniMax TTS API client for generating pronunciation audio."""

import os

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import config


class MiniMaxTTSError(Exception):
    """Raised when TTS generation fails."""

    pass


class MiniMaxRateLimitError(MiniMaxTTSError):
    """Raised when rate limited by the API."""

    pass


def get_api_key() -> str:
    """Get MiniMax API key from environment variable."""
    key = os.environ.get("MINIMAX_API_KEY")
    if not key:
        raise MiniMaxTTSError("MINIMAX_API_KEY environment variable not set")
    return key


def create_client() -> httpx.Client:
    """Create a reusable httpx client for MiniMax API calls."""
    return httpx.Client(timeout=config.MINIMAX_TTS_TIMEOUT)


@retry(
    stop=stop_after_attempt(config.MINIMAX_TTS_MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError, MiniMaxRateLimitError)),
)
def generate_audio(api_key: str, voice_id: str, text: str, client: httpx.Client | None = None) -> tuple[bytes, dict]:
    """Call MiniMax TTS API and return MP3 audio bytes and extra info.

    Args:
        api_key: MiniMax API key
        voice_id: MiniMax voice ID (e.g. "English_CalmWoman")
        text: Text to synthesize
        client: Optional reusable httpx.Client for connection pooling

    Returns:
        Tuple of (audio_bytes, extra_info_dict)

    Raises:
        MiniMaxTTSError: If generation fails
        MiniMaxRateLimitError: If rate limited (will be retried)
    """
    payload = {
        "model": config.MINIMAX_TTS_MODEL,
        "text": text,
        "voice_setting": {
            "voice_id": voice_id,
            "speed": 1.0,
        },
        "audio_setting": {
            "format": "mp3",
            "sample_rate": 24000,
            "bitrate": 128000,
            "channel": 1,
        },
        "language_boost": "en",
    }

    if client is None:
        client = create_client()

    resp = client.post(
        f"{config.MINIMAX_API_BASE}/t2a_v2",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    resp.raise_for_status()
    data = resp.json()

    status_code = data.get("base_resp", {}).get("status_code", -1)
    status_msg = data.get("base_resp", {}).get("status_msg", "unknown")

    if status_code == 1002:
        raise MiniMaxRateLimitError(f"Rate limited: {status_msg}")

    if status_code != 0:
        raise MiniMaxTTSError(f"API error (code {status_code}): {status_msg}")

    audio_hex = data.get("data", {}).get("audio")
    if not audio_hex:
        raise MiniMaxTTSError("No audio data in response")

    audio_bytes = bytes.fromhex(audio_hex)
    extra_info = data.get("extra_info", {})

    return audio_bytes, extra_info
