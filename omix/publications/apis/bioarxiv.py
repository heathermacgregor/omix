"""
BioRxiv / MedRxiv API wrapper (via Crossref).

BioRxiv preprints are indexed by Crossref with a specific prefix, so the
standard Crossref API can be filtered to return only bioRxiv content.
"""

from typing import Any, Dict, List

from .crossref import CrossrefAPI
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.bioarxiv")


class BioarxivAPI(CrossrefAPI):
    """Searches bioRxiv / medRxiv preprints using the Crossref API with a prefix filter."""

    def __init__(
        self,
        email: str,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 32.0,
    ):
        super().__init__(
            email,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
        )
        self._source_name = "bioarxiv"

    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search for preprints matching `query` on bioRxiv / medRxiv.

        Uses the Crossref API with a filter for the known DOI prefixes of
        bioRxiv (10.1101) and medRxiv (10.1101).

        Returns:
            List of publication dicts with keys: doi, publication_title, pub_year, status
        """
        # Override base_url temporarily to include the prefix filter.
        original_url = self.base_url
        try:
            # The Crossref API allows a filter on the prefix via query param.
            # We keep the existing `search` implementation and just add the filter.
            self.base_url = "https://api.crossref.org/works"
            params = {
                "query": query,
                "rows": limit,
                "mailto": self.email,
                "sort": "published",
                "order": "asc",
                "filter": "prefix:10.1101",  # bioRxiv/medRxiv
            }
            self._rate_limit('crossref')
            resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()

            publications: List[Dict[str, Any]] = []
            for item in data.get('message', {}).get('items', []):
                title_list = item.get('title') or ["Unknown Title"]
                title = title_list[0]
                date_parts = item.get('issued', {}).get('date-parts', [[]])
                year = date_parts[0][0] if date_parts and date_parts[0] else None
                publications.append({
                    "doi": item.get('DOI'),
                    "publication_title": title,
                    "pub_year": str(year) if year else "N/A",
                    "status": "Ready (bioRxiv/medRxiv)",
                })
            return publications
        finally:
            self.base_url = original_url