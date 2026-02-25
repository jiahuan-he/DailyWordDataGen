"""Pydantic data models for the DailyWord data generation pipeline."""

from typing import Optional
from pydantic import BaseModel, Field


class SelectedWord(BaseModel):
    """A word selected for inclusion in the vocabulary list."""

    word: str


class EnrichedWord(BaseModel):
    """A word enriched with phonetic and part-of-speech information."""

    word: str
    phonetic: Optional[str] = None
    pos: list[str] = Field(default_factory=list)


class ExampleSentence(BaseModel):
    """An example sentence with translation."""

    sentence: str
    style: str
    translation: str
    translated_word: str
    display_order: Optional[int] = None  # 1-4 for selected examples, None for others


class FinalWordEntry(BaseModel):
    """Complete word entry with all generated data."""

    word: str
    phonetic: Optional[str] = None
    pos: list[str] = Field(default_factory=list)
    selected_pos: str
    definition: str
    examples: list[ExampleSentence]


class LLMGenerationResult(BaseModel):
    """Result from LLM generation for a single word."""

    selected_pos: str
    definition: str
    examples: list[ExampleSentence]


class CheckpointData(BaseModel):
    """Checkpoint data for tracking progress."""

    processed_words: list[str] = Field(default_factory=list)
    failed_words: list[str] = Field(default_factory=list)
    last_index: int = 0
