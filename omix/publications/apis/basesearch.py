"""
BASE (Bielefeld Academic Search Engine) API wrapper.

Implements `PublicationSource` using the BASE REST API.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.basesearch")


class BaseSearchAPI(BasePublicationAPI):
    """Searches the BASE API for publications matching a query string."""

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
        self._source_name = "basesearch"
        self.base_url = "https://api.base-search.net/v2/search"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search BASE for publications matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "q": query,
            "format": "json",
            "sort": "date:asc",
            "limit": limit,
        }
        self._rate_limit('basesearch')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)

        if resp.status_code == 204:
            return []
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        docs = data.get('response', {}).get('docs', [])
        for item in docs:
            year = item.get('year') or str(item.get('date', 'N/A'))[:4]

            doi = item.get('doi')
            if isinstance(doi, list):
                doi = doi[0] if doi else None

            publications.append({
                "doi": doi,
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": str(year),
                "status": "Ready (BASE)",
            })
        return publications