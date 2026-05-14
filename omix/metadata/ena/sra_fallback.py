"""SRA fallback – fetch sample metadata from NCBI SRA when ENA has no data."""
import pandas as pd
from typing import Optional
from omix.metadata.ena.cache import SQLiteCacheManager
from omix.logging_utils import get_logger

logger = get_logger("omix.ena.sra")

async def fetch_sra_samples_for_project(
    project_accession: str,
    email: str,
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> pd.DataFrame:
    """
    Use NCBI E‑utilities to get SRA run accessions for a BioProject,
    then return a minimal DataFrame with sample metadata.
    """
    # Placeholder – real implementation would call eutils
    logger.warning("SRA fallback not yet implemented – returning empty.")
    return pd.DataFrame()