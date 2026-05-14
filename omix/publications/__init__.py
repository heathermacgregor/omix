"""
Publication search & analysis subpackage.

This subpackage is responsible for:
- Searching for publications linked to study/sample accessions across multiple APIs
  (Crossref, Europe PMC, NCBI, Semantic Scholar, etc.).
- Downloading and extracting full‑text from PDFs and web pages.
- Cleaning and parsing text to locate methodology sections, citations, and DNA sequences.
- Running LLM‑powered extraction of experimental details (primers, kits, sequencing platforms).
- Validating extracted information against reference databases.
- Caching all results in SQLite.
"""

from .base import PublicationSource, OmicsExtractor
from .fetcher import PublicationFetcher
from .cache import PublicationCache