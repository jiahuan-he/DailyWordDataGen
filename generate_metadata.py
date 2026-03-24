"""Generate app metadata files (word_order.json, word_levels.json) from vocabulary CSV."""

import argparse
import json
from pathlib import Path

import config
from src.step2_enrichment import load_vocabulary_words


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate word_order.json and word_levels.json for DailyWord app."
    )
    parser.add_argument(
        "--level-tiers", "-f",
        required=True,
        help="Comma-separated frequency values defining each level, e.g. 9,8,6,4 means Level 1=freq 9, Level 2=freq 8, etc.",
    )
    args = parser.parse_args()

    frequencies = [int(f.strip()) for f in args.level_tiers.split(",")]

    all_words = load_vocabulary_words(config.VOCABULARY_CSV)
    words = [
        w for w in all_words
        if w.output_file and (config.PROJECT_ROOT / w.output_file).exists()
    ]
    print(f"Loaded {len(all_words)} words, {len(words)} have existing output files")

    # word_order.json: processed words in CSV order
    word_order = [w.word for w in words]
    word_order_path = config.VOCABULARY_CSV.parent / "word_order.json"
    with open(word_order_path, "w") as f:
        json.dump(word_order, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {len(word_order)} words to {word_order_path}")

    # word_levels.json: map frequency tiers to level numbers with starting indices
    levels = []
    for level_num, freq in enumerate(frequencies, start=1):
        # Find the index of the first word with this frequency
        index = next(
            (i for i, w in enumerate(words) if w.frequency == freq),
            None,
        )
        if index is None:
            print(f"Warning: no words found with frequency {freq}, skipping level {level_num}")
            continue
        levels.append({
            "level": level_num,
            "frequency": freq,
            "startingWordIndex": index,
        })

    word_levels_path = config.VOCABULARY_CSV.parent / "word_levels.json"
    with open(word_levels_path, "w") as f:
        json.dump(levels, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {len(levels)} levels to {word_levels_path}")


if __name__ == "__main__":
    main()
