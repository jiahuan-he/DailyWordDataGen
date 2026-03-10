"""Configuration settings for DailyWord data generation pipeline."""

import re
from datetime import datetime
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
FINAL_DATA_DIR = PROJECT_ROOT / "final_data_v2"  # Step 3 output directory
CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"
PROMPTS_DIR = PROJECT_ROOT / "prompts"
LOGS_DIR = PROJECT_ROOT / "logs"
TEST_OUTPUT_DIR = PROJECT_ROOT / "test_output"

# Input/Output files
VOCABULARY_CSV = PROJECT_ROOT / "source" / "word_frequencies_sorted_v2.csv"
ENRICHED_WORDS_JSON = DATA_DIR / "enriched_words.json"


def get_final_output_path(
    timestamp: datetime | None = None, word_range: tuple[int, int] | None = None
) -> Path:
    """Generate output path with datetime suffix.

    Args:
        timestamp: Datetime to use for suffix. If None, uses current time.
        word_range: Optional word range tuple (start, end) for subfolder organization.

    Returns:
        Path like final_data_v2/final_output_20260131_143022.json
        or final_data_v2/201-300/final_output_20260131_143022.json if word_range provided
    """
    if timestamp is None:
        timestamp = datetime.now()
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    if word_range:
        range_folder = f"{word_range[0]}-{word_range[1]}"
        return FINAL_DATA_DIR / range_folder / f"final_output_{suffix}.json"
    return FINAL_DATA_DIR / f"final_output_{suffix}.json"

# Checkpoint files
STEP2_CHECKPOINT = CHECKPOINTS_DIR / "step2_progress.json"
STEP3_CHECKPOINT = CHECKPOINTS_DIR / "step3_progress.json"


def get_step2_checkpoint_path(word_range: tuple[int, int] | None = None) -> Path:
    """Get checkpoint path for Step 2, optionally scoped to a word range."""
    if word_range:
        return CHECKPOINTS_DIR / f"step2_progress_{word_range[0]}-{word_range[1]}.json"
    return STEP2_CHECKPOINT


def get_step3_checkpoint_path(word_range: tuple[int, int] | None = None) -> Path:
    """Get checkpoint path for Step 3, optionally scoped to a word range."""
    if word_range:
        return CHECKPOINTS_DIR / f"step3_progress_{word_range[0]}-{word_range[1]}.json"
    return STEP3_CHECKPOINT


def get_enriched_words_path(word_range: tuple[int, int] | None = None) -> Path:
    """Get enriched words output path, optionally scoped to a word range."""
    if word_range:
        return DATA_DIR / f"enriched_words_{word_range[0]}-{word_range[1]}.json"
    return ENRICHED_WORDS_JSON

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
    timestamp: datetime | None = None,
    word_range: tuple[int, int] | None = None,
) -> Path:
    """Generate test output path under test_output/<model>/<range>/."""
    if timestamp is None:
        timestamp = datetime.now()
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    base = TEST_OUTPUT_DIR / model_short
    if word_range:
        base = base / f"{word_range[0]}-{word_range[1]}"
    return base / f"final_output_{suffix}.json"


def get_test_checkpoint_dir(model_short: str) -> Path:
    """Get test checkpoint directory under test_output/<model>/checkpoints/."""
    return TEST_OUTPUT_DIR / model_short / "checkpoints"
