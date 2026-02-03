"""Logging configuration for DailyWord data generation pipeline."""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Log directory
LOGS_DIR = Path(__file__).parent.parent / "logs"


def setup_logger(
    name: str = "dailyword",
    log_file: str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Set up and return a configured logger.

    Args:
        name: Logger name
        log_file: Optional specific log file name. If None, generates timestamp-based name.
        level: Logging level

    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Generate log file name with timestamp if not provided
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"run_{timestamp}.log"

    log_path = LOGS_DIR / log_file

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_formatter = logging.Formatter(
        "%(message)s"
    )

    # File handler - captures everything
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler - for user-facing output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Log the log file location
    logger.info(f"Log file: {log_path}")

    return logger


def get_logger(name: str = "dailyword") -> logging.Logger:
    """
    Get an existing logger by name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Convenience function for batch processing
def setup_batch_logger(batch_index: int) -> logging.Logger:
    """
    Set up logger for batch processing with batch-specific log file.

    Args:
        batch_index: The batch index being processed

    Returns:
        Configured logger instance
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"batch_{batch_index:03d}_{timestamp}.log"
    return setup_logger(name="batch", log_file=log_file)
