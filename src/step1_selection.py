"""Step 1: Word Selection - Filter words marked for inclusion."""

import pandas as pd
from pathlib import Path

import config
from src.models import SelectedWord


def filter_selected_words(
    input_path: Path = config.WORD_SELECTION_CSV,
    output_path: Path = config.SELECTED_WORDS_CSV,
) -> list[SelectedWord]:
    """
    Filter words from word_selection.csv where include == 'Y'.

    Args:
        input_path: Path to the input CSV file
        output_path: Path to save the filtered CSV

    Returns:
        List of SelectedWord objects
    """
    df = pd.read_csv(input_path)

    # Filter rows where include is 'Y' and word is not null
    selected_df = df[df["include"] == "Y"][["frequency", "word"]].copy()
    selected_df = selected_df.dropna(subset=["word"])

    # Sort by frequency (lower frequency = more common word)
    selected_df = selected_df.sort_values("frequency").reset_index(drop=True)

    # Save to output CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    selected_df.to_csv(output_path, index=False)

    # Convert to list of SelectedWord objects
    words = [
        SelectedWord(frequency=row["frequency"], word=row["word"])
        for _, row in selected_df.iterrows()
    ]

    return words


def load_selected_words(path: Path = config.SELECTED_WORDS_CSV) -> list[SelectedWord]:
    """Load previously selected words from CSV."""
    df = pd.read_csv(path)
    return [
        SelectedWord(frequency=row["frequency"], word=row["word"])
        for _, row in df.iterrows()
    ]


def run_step1() -> list[SelectedWord]:
    """Run Step 1 of the pipeline."""
    print("Step 1: Filtering selected words...")
    words = filter_selected_words()
    print(f"  Selected {len(words)} words for processing")
    print(f"  Output saved to: {config.SELECTED_WORDS_CSV}")
    return words


if __name__ == "__main__":
    run_step1()
