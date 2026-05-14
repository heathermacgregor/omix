"""
Omics‑specific methodology extraction rules and LLM prompts.

Each module implements the `OmicsExtractor` interface for a particular
omics type (16S, metagenomics, etc.).
"""

from .base import OmicsExtractor
from ._16s import SixteenSExtractor