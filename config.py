"""Configuration settings for DailyWord data generation pipeline."""

import re
from datetime import datetime
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent
FINAL_DATA_DIR = PROJECT_ROOT / "final_data_v3"  # Step 3 output directory (per-word)
CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"
PROMPTS_DIR = PROJECT_ROOT / "prompts"
LOGS_DIR = PROJECT_ROOT / "logs"
TEST_OUTPUT_DIR = PROJECT_ROOT / "test_output"

# Input/Output files
VOCABULARY_CSV = PROJECT_ROOT / "source" / "word_frequencies_sorted.csv"


def _safe_word(word: str) -> str:
    """Sanitize a word for use in file/directory names."""
    return re.sub(r'[^\w\-]', '_', word)


def get_word_output_path(word: str, timestamp: datetime | None = None) -> Path:
    """Generate per-word output path.

    Returns:
        Path like final_data_v3/{safe_word}/{safe_word}_{timestamp}.json
    """
    if timestamp is None:
        timestamp = datetime.now()
    safe = _safe_word(word)
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    return FINAL_DATA_DIR / safe / f"{safe}_{suffix}.json"


# Prompt templates
EXAMPLE_GENERATION_PROMPT = PROMPTS_DIR / "example_generation.txt"
EXAMPLE_SELECTION_PROMPT = PROMPTS_DIR / "example_selection.txt"
TRANSLATION_ENRICHMENT_PROMPT = PROMPTS_DIR / "translation_enrichment.txt"

# API settings
FREE_DICTIONARY_API_URL = "https://api.dictionaryapi.dev/api/v2/entries/en"
DICTIONARY_API_TIMEOUT = 10  # seconds
DICTIONARY_API_MAX_RETRIES = 5

# Claude CLI settings
CLAUDE_MODEL = "claude-opus-4-5-20251101"
CLAUDE_TIMEOUT = 180  # seconds

# Processing settings
DEFAULT_BATCH_SIZE = 100
DRY_RUN_LIMIT = 10

# Example styles (7 styles, one sentence each)
EXAMPLE_STYLES = [
    "Formal",
    "Definitional",
    "Contrastive",
    "Collocational",
    "Philosophical",
    "Poetic",
    "Inspirational",
]


def model_short_name(model_id: str) -> str:
    """Extract short name from a Claude model ID.

    E.g., 'claude-sonnet-4-5-20250514' → 'sonnet-4-5'
         'claude-opus-4-5-20251101' → 'opus-4-5'
         'claude-haiku-4-5-20251001' → 'haiku-4-5'
    """
    # Strip 'claude-' prefix and date suffix (YYYYMMDD)
    name = re.sub(r"^claude-", "", model_id)
    name = re.sub(r"-\d{8}$", "", name)
    return name


def get_test_output_path(
    model_short: str,
    word: str,
    timestamp: datetime | None = None,
) -> Path:
    """Generate test output path under test_output/<model>/<word>/<word>_{timestamp}.json."""
    if timestamp is None:
        timestamp = datetime.now()
    safe = _safe_word(word)
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    return TEST_OUTPUT_DIR / model_short / safe / f"{safe}_{suffix}.json"
