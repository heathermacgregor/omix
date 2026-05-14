"""
PLOS API wrapper.

Implements `PublicationSource` using the PLOS (Public Library of Science) Search API.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.plos")


class PLOSAPI(BasePublicationAPI):
    """Searches PLOS for articles matching a query string."""

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
        self._source_name = "plos"
        self.base_url = "http://api.plos.org/search"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search PLOS for articles matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "q": f'"{query}"',
            "fl": "id,publication_date,title",
            "wt": "json",
            "rows": limit,
            "sort": "publication_date asc",
        }
        self._rate_limit('plos')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('response', {}).get('docs', []):
            year = str(item.get('publication_date', 'N/A'))[:4]
            publications.append({
                "doi": item.get('id'),
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": year,
                "status": "Ready (PLOS)",
            })
        return publications