# DailyWord Data Generation Pipeline

Generates vocabulary data for DailyWord iOS app: phonetics, POS, Chinese definitions, and 7 styled example sentences with translations.

## Setup
```bash
source venv/bin/activate
```

## Pipeline Steps
1. **Step 2**: Enrich with phonetics/POS from Free Dictionary API (in-memory)
2. **Step 3**: Generate examples via Claude CLI → `final_data_v4/{word}/{word}_{timestamp}.json`

Word source: `source/word_frequencies_sorted_v4.csv` (12,052 words). The `output_file` column tracks completion.

## Usage
```bash
python main.py --dry-run              # Test with 10 words (no CSV update)
python main.py --count 50             # Process 50 unprocessed words
python main.py                        # Process next 100 unprocessed words

# Batch processing
python batch_process.py --count 50 --batches 3
```

## TTS Audio Generation
Generates MP3 pronunciation audio using MiniMax TTS API. Requires `MINIMAX_API_KEY` env var.

Voices configured in `config.py` (`VOICES` dict). Tracked via `source/audio_generation_tracker.csv`.

```bash
python generate_audio.py --status                      # Show progress
python generate_audio.py --count 50                    # Generate 50 words, default voice
python generate_audio.py --voice all --count 50        # All voices, 50 words each
python generate_audio.py --words ability,abandon       # Specific words
python generate_audio.py --count 50 -f 9,8             # 50 words per frequency tier
python generate_audio.py --dry-run --count 10          # Preview
```

Output: `audio_data/{voice_key}/{word}/` with `word.mp3`, `sentence_1-4.mp3`, `metadata.json`.

## S3 Upload
```bash
python upload_to_s3.py                                         # Upload new words only (incremental)
python upload_to_s3.py --force                                 # Force re-upload all words
python upload_to_s3.py --wipe-and-upload                       # Wipe bucket and re-upload all words + metadata
python upload_to_s3.py --words abandon,aim                     # Upload specific words (if not in S3)
python upload_to_s3.py --dry-run                               # Preview uploads
python upload_to_s3.py --audio                                 # Upload new audio (incremental)
python upload_to_s3.py --audio --voice american_woman_calm     # Upload audio for specific voice
python upload_to_s3.py --voice-registry                        # Upload voice_registry.json
```

## Key Files
- `config.py` - All configuration settings (including TTS voices)
- `src/models.py` - Pydantic data models
- `src/minimax_client.py` - MiniMax TTS API client
- `prompts/example_generation.txt` - LLM prompt template
- `source/word_frequencies_sorted_v4.csv` - Word list with progress tracking
- `source/audio_generation_tracker.csv` - TTS audio generation progress
