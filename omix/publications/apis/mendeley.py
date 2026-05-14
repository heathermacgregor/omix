"""
Mendeley API wrapper.

Implements `PublicationSource` using the Mendeley Catalog API.
"""

from typing import Any, Dict, List, Optional

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.mendeley")


class MendeleyAPI(BasePublicationAPI):
    """Searches Mendeley for publications matching a query string."""

    def __init__(
        self,
        email: str,
        api_key: Optional[str] = None,
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
        self._source_name = "mendeley"
        self.base_url = "https://api.mendeley.com/catalog"
        self.api_key = api_key

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Mendeley for publications matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        if not self.api_key:
            logger.debug("Mendeley API key missing; skipping")
            return []

        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = {
            "query": f'"{query}"',
            "view": "all",
            "limit": limit,
            "sort": "year",
            "direction": "asc",
        }
        self._rate_limit('mendeley')
        resp = self.session.get(
            self.base_url, headers=headers, params=params, timeout=self.timeout
        )
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data:
            doi = item.get('identifiers', {}).get('doi')
            publications.append({
                "doi": doi,
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": str(item.get('year', 'N/A')),
                "status": "Ready (Mendeley)",
            })
        return publications