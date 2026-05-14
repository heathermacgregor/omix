"""
Crossref API wrapper.

Implements `PublicationSource` using the Crossref REST API.
"""

from typing import Any, Dict, List, Optional

from omix.logging_utils import get_logger
from .base import BasePublicationAPI, with_http_backoff

logger = get_logger("omix.apis.crossref")


class CrossrefAPI(BasePublicationAPI):
    """Searches the Crossref API for publications linked to an accession string."""

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
        self._source_name = "crossref"
        self.base_url = "https://api.crossref.org/works"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Crossref for publications matching the query.

        Args:
            query: Free‑text search query (e.g., an accession, title, or keyword).
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "query": query,
            "rows": limit,
            "mailto": self.email,
            "sort": "published",
            "order": "asc",
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
                "status": "Ready (Crossref)",
            })
        return publications