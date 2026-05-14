"""
CORE API wrapper.

Implements `PublicationSource` using the CORE API (core.ac.uk) to search
for open‑access publications.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.core")


class CoreAPI(BasePublicationAPI):
    """Searches CORE for open‑access publications matching a query string."""

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
        self._source_name = "core"
        self.base_url = "https://api.core.ac.uk/v3/search/works"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search CORE for publications matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        payload = {
            "q": query,
            "limit": limit,
            "sort": "yearPublished:asc",
        }
        self._rate_limit('core')
        resp = self.session.post(self.base_url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('results', []):
            publications.append({
                "doi": item.get('doi'),
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": str(item.get('yearPublished', 'N/A')),
                "status": "Ready (CORE)",
            })
        return publications