"""
omix: A modular Python package for fetching, enriching, and analyzing
omics metadata and publications.

Usage:
    from omix import Config
    config = Config(email="you@example.com")
"""

__version__ = "0.1.0"

from .config import Config, load_config
from .logging_utils import setup_logging, get_logger