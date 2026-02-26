"""Checkpoint system for tracking pipeline progress."""

import fcntl
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from src.models import CheckpointData


class CheckpointManager:
    """Manages checkpointing for pipeline steps."""

    def __init__(self, checkpoint_path: Path):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_path: Path to the checkpoint JSON file
        """
        self.checkpoint_path = checkpoint_path
        self._lock_path = checkpoint_path.with_suffix(".lock")
        self._data: Optional[CheckpointData] = None

    @contextmanager
    def _file_lock(self):
        """Context manager for file locking using fcntl."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, "w") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def load(self) -> CheckpointData:
        """Load checkpoint data from file, or create new if not exists."""
        if self._data is not None:
            return self._data

        with self._file_lock():
            if self.checkpoint_path.exists():
                with open(self.checkpoint_path, "r") as f:
                    data = json.load(f)
                    self._data = CheckpointData(**data)
            else:
                self._data = CheckpointData()

        return self._data

    def save(self) -> None:
        """Save current checkpoint data to file."""
        if self._data is None:
            return

        with self._file_lock():
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.checkpoint_path, "w") as f:
                json.dump(self._data.model_dump(), f, indent=2)

    def mark_processed(self, word: str, index: int) -> None:
        """
        Mark a word as successfully processed.

        Args:
            word: The word that was processed
            index: The index of the word in the list
        """
        data = self.load()
        if word not in data.processed_words:
            data.processed_words.append(word)
        data.last_index = index
        self.save()

    def mark_failed(self, word: str) -> None:
        """
        Mark a word as failed.

        Args:
            word: The word that failed processing
        """
        data = self.load()
        if word not in data.failed_words:
            data.failed_words.append(word)
        self.save()

    def is_processed(self, word: str) -> bool:
        """Check if a word has been processed."""
        data = self.load()
        return word in data.processed_words

    def get_unprocessed_indices(self, total_count: int) -> list[int]:
        """
        Get indices of words that haven't been processed yet.

        Args:
            total_count: Total number of words

        Returns:
            List of indices to process
        """
        data = self.load()
        processed_count = len(data.processed_words)
        return list(range(processed_count, total_count))

    def get_failed_words(self) -> list[str]:
        """Get list of words that failed processing."""
        data = self.load()
        return data.failed_words.copy()

    def clear_failed(self) -> None:
        """Clear the failed words list for retry."""
        data = self.load()
        data.failed_words = []
        self.save()

    def reset(self) -> None:
        """Reset checkpoint to initial state."""
        self._data = CheckpointData()
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()

    @property
    def processed_count(self) -> int:
        """Get number of processed words."""
        data = self.load()
        return len(data.processed_words)

    @property
    def failed_count(self) -> int:
        """Get number of failed words."""
        data = self.load()
        return len(data.failed_words)
