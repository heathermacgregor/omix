"""
DOAJ (Directory of Open Access Journals) API wrapper.

Implements `PublicationSource` using the DOAJ API.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.doaj")


class DOAJAPI(BasePublicationAPI):
    """Searches the DOAJ for open‑access articles matching a query string."""

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
        self._source_name = "doaj"
        self.base_url = "https://doaj.org/api/articles/search"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search DOAJ for articles matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "q": query,
            "pageSize": limit,
            "sort": "created_date:asc",
        }
        self._rate_limit('doaj')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('results', []):
            bibjson = item.get('bibjson', {})
            # Extract DOI from the identifier list
            doi = None
            for identifier in bibjson.get('identifier', []):
                if identifier.get('type') == 'doi':
                    doi = identifier.get('id')
                    break

            publications.append({
                "doi": doi,
                "publication_title": bibjson.get('title', 'Unknown Title'),
                "pub_year": str(bibjson.get('year', 'N/A')),
                "status": "Ready (DOAJ)",
            })
        return publications