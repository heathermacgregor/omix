"""
Abstract base classes for metadata sources and enrichment steps.

All metadata fetchers (ENA, GEO, SRA, etc.) and enrichers must implement
these interfaces so that the rest of the pipeline can work with any source.
"""

from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class AbstractMetadataFetcher(ABC):
    """
    Interface for fetching raw metadata from a public database.

    Subclasses must implement:
      - fetch_project_metadata(): retrieve metadata for one or more project/study IDs.
      - close(): release network sessions, connections, and caches.
    """

    @abstractmethod
    async def fetch_project_metadata(self, project_ids: List[str]) -> pd.DataFrame:
        """
        Return a combined metadata DataFrame for one or more project/study accessions.

        Args:
            project_ids: List of project/study accessions (e.g. PRJNA864623, SRP123456).

        Returns:
            A pandas DataFrame with all available metadata for every sample in the projects.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release any resources (HTTP sessions, database connections, etc.)."""
        ...


class AbstractMetadataEnricher(ABC):
    """
    Interface for enriching an existing DataFrame with additional data.

    Subclasses might:
      - reverse‑geocode coordinates,
      - look up ontology labels,
      - fetch publication DOIs,
      - add host/environment categories,
    """

    @abstractmethod
    async def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform enrichment and return the modified DataFrame.

        Args:
            df: Input DataFrame (already containing basic metadata).

        Returns:
            The same DataFrame with additional columns added or filled.
        """
        ...