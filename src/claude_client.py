"""Claude CLI wrapper for LLM generation."""

import json
import subprocess
from typing import Optional

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config
from src.models import LLMGenerationResult, ExampleSentence


class ClaudeGenerationError(Exception):
    """Raised when Claude generation fails."""

    pass


class ClaudeTimeoutError(ClaudeGenerationError):
    """Raised when Claude generation times out."""

    pass


class ClaudeParseError(ClaudeGenerationError):
    """Raised when Claude response cannot be parsed."""

    pass


class ClaudeConsecutiveFailureError(ClaudeGenerationError):
    """Raised when Claude fails consecutively, indicating systemic issues like rate limits."""

    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception_type((ClaudeTimeoutError,)),
)
def generate_with_claude(prompt: str, timeout: int = config.CLAUDE_TIMEOUT) -> dict:
    """
    Generate content using Claude CLI in headless mode.

    Args:
        prompt: The prompt to send to Claude
        timeout: Timeout in seconds

    Returns:
        Parsed JSON response from Claude

    Raises:
        ClaudeGenerationError: If generation fails
        ClaudeTimeoutError: If generation times out
        ClaudeParseError: If response cannot be parsed
    """
    try:
        result = subprocess.run(
            [
                "claude",
                "-p",
                prompt,
                "--model",
                config.CLAUDE_MODEL,
                "--output-format",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise ClaudeTimeoutError(f"Claude generation timed out after {timeout}s")
    except FileNotFoundError:
        raise ClaudeGenerationError("Claude CLI not found. Please install claude-code CLI.")

    if result.returncode != 0:
        raise ClaudeGenerationError(f"Claude CLI error: {result.stderr}")

    # Parse the JSON output
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ClaudeParseError(f"Failed to parse Claude response as JSON: {e}")

    # Extract the actual content from the response
    # The --output-format json wraps the response in a structure
    if isinstance(response, dict) and "result" in response:
        content = response["result"]
    elif isinstance(response, dict) and "content" in response:
        content = response["content"]
    else:
        content = result.stdout

    # Try to extract JSON from the content
    return extract_json_from_response(content)


def extract_json_from_response(content: str) -> dict:
    """
    Extract JSON object from Claude's response.

    The response might contain markdown code blocks or other text.

    Args:
        content: Raw response content

    Returns:
        Parsed JSON dictionary
    """
    if isinstance(content, dict):
        return content

    # Try direct JSON parse first
    try:
        return json.loads(content)
    except (json.JSONDecodeError, TypeError):
        pass

    # Try to find JSON in markdown code block
    import re

    # Look for ```json ... ``` blocks
    json_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", content)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Look for raw JSON object
    json_match = re.search(r"\{[\s\S]*\}", content)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    raise ClaudeParseError(f"Could not extract JSON from response: {content[:500]}...")


def parse_generation_result(data: dict) -> LLMGenerationResult:
    """
    Parse Claude's response into LLMGenerationResult.

    Args:
        data: Parsed JSON response

    Returns:
        LLMGenerationResult with selected_pos, definition and examples
    """
    if "selected_pos" not in data:
        raise ClaudeParseError("Response missing 'selected_pos' field")
    if "definition" not in data:
        raise ClaudeParseError("Response missing 'definition' field")
    if "examples" not in data:
        raise ClaudeParseError("Response missing 'examples' field")

    examples = []
    for ex in data["examples"]:
        examples.append(
            ExampleSentence(
                sentence=ex.get("sentence", ""),
                style=ex.get("style", ""),
                translation=ex.get("translation", ""),
                translated_word=ex.get("translated_word", ""),
                # Score fields - use .get() for backward compatibility
                contextual_fitness=ex.get("contextual_fitness"),
                memorability=ex.get("memorability"),
                emotional_resonance=ex.get("emotional_resonance"),
            )
        )

    return LLMGenerationResult(
        selected_pos=data["selected_pos"],
        definition=data["definition"],
        examples=examples,
    )


def generate_examples_for_word(
    word: str,
    pos: list[str],
    prompt_template: str,
) -> LLMGenerationResult:
    """
    Generate examples for a word using Claude.

    Args:
        word: The word to generate examples for
        pos: List of parts of speech
        prompt_template: The prompt template to use

    Returns:
        LLMGenerationResult with generated content
    """
    pos_str = ", ".join(pos) if pos else "unknown"
    prompt = prompt_template.format(word=word, pos=pos_str)

    response = generate_with_claude(prompt)
    return parse_generation_result(response)
