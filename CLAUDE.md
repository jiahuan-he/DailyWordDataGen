# DailyWord Data Generation Pipeline

Generates vocabulary data for DailyWord iOS app: phonetics, POS, Chinese definitions, and 11 styled example sentences with translations.

## Setup
```bash
source venv/bin/activate
```

## Pipeline Steps
1. **Step 2**: Enrich with phonetics/POS from Free Dictionary API → `data/enriched_words.json`
2. **Step 3**: Generate examples via Claude CLI → `data/final_output.json`

Word source: `data/word_frequencies_sorted.csv` (12,059 words)

## Usage
```bash
python main.py --dry-run              # Test with 10 words
python main.py --word-range 0-100     # Process subset (parallel-safe)
python main.py --resume               # Resume from checkpoint

# Parallel execution (each uses separate checkpoint/output files)
python main.py --word-range 200-300   # Terminal 1
python main.py --word-range 300-400   # Terminal 2
```

## Key Files
- `config.py` - All configuration settings
- `src/models.py` - Pydantic data models
- `prompts/example_generation.txt` - LLM prompt template
- `checkpoints/` - Progress tracking for resume capability
