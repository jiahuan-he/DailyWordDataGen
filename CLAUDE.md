# DailyWord Data Generation Pipeline

Generates vocabulary data for DailyWord iOS app: phonetics, POS, Chinese definitions, and 7 styled example sentences with translations.

## Setup
```bash
source venv/bin/activate
```

## Pipeline Steps
1. **Step 2**: Enrich with phonetics/POS from Free Dictionary API (in-memory)
2. **Step 3**: Generate examples via Claude CLI → `final_data_v3/{word}/{word}_{timestamp}.json`

Word source: `source/word_frequencies_sorted.csv` (12,052 words). The `output_file` column tracks completion.

## Usage
```bash
python main.py --dry-run              # Test with 10 words (no CSV update)
python main.py --count 50             # Process 50 unprocessed words
python main.py                        # Process next 100 unprocessed words

# Batch processing
python batch_process.py --count 50 --batches 3
```

## Key Files
- `config.py` - All configuration settings
- `src/models.py` - Pydantic data models
- `prompts/example_generation.txt` - LLM prompt template
- `source/word_frequencies_sorted.csv` - Word list with progress tracking
