"""
Centralized logging utilities for omix.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger configured with a standard format.
    If the logger already has handlers, returns it unchanged.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Avoid duplicate output when root handlers are configured via setup_logging.
        logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.NOTSET)
    return logger


def setup_logging(log_dir: Optional[Path] = None, level: int = logging.INFO) -> None:
    """
    Configure root logger with console and optional file output.

    Args:
        log_dir: Directory for log files; if provided, enables file logging.
        level: Logging level (default: INFO).
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Avoid adding duplicate handlers
    if root_logger.handlers:
        return

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "omix.log")
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-7s | %(name)s | [%(filename)s:%(lineno)d] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root_logger.addHandler(file_handler)