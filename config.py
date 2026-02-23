"""Configuration settings for DailyWord data generation pipeline."""

from datetime import datetime
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CHECKPOINTS_DIR = PROJECT_ROOT / "checkpoints"
PROMPTS_DIR = PROJECT_ROOT / "prompts"
LOGS_DIR = PROJECT_ROOT / "logs"

# Input/Output files
WORD_SELECTION_CSV = PROJECT_ROOT / "word_selection.csv"
SELECTED_WORDS_CSV = DATA_DIR / "selected_words.csv"
ENRICHED_WORDS_JSON = DATA_DIR / "enriched_words.json"


def get_final_output_path(timestamp: datetime | None = None) -> Path:
    """Generate output path with datetime suffix.

    Args:
        timestamp: Datetime to use for suffix. If None, uses current time.

    Returns:
        Path like data/final_output_20260131_143022.json
    """
    if timestamp is None:
        timestamp = datetime.now()
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"final_output_{suffix}.json"

# Checkpoint files
STEP2_CHECKPOINT = CHECKPOINTS_DIR / "step2_progress.json"
STEP3_CHECKPOINT = CHECKPOINTS_DIR / "step3_progress.json"

# Prompt template
EXAMPLE_GENERATION_PROMPT = PROMPTS_DIR / "example_generation.txt"

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

# Example styles (9 styles, one sentence each)
EXAMPLE_STYLES = [
    "Formal",
    "Definitional",
    "Contrastive",
    "Collocational",
    "Philosophical",
    "Warm",
    "Poetic",
    "Inspirational",
    "News-like",
]
