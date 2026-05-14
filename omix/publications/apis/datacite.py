"""
DataCite API wrapper.

Implements `PublicationSource` using the DataCite REST API.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.datacite")


class DataciteAPI(BasePublicationAPI):
    """Searches DataCite for publications / datasets matching a query string."""

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
        self._source_name = "datacite"
        self.base_url = "https://api.datacite.org/works"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search DataCite for works matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "query": query,
            "page[size]": limit,
            "sort": "published",
        }
        self._rate_limit('datacite')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('data', []):
            attrs = item.get('attributes', {})
            title_list = attrs.get('titles', [])
            title = title_list[0].get('title', 'Unknown Title') if title_list else "Unknown Title"
            year = attrs.get('publicationYear', 'N/A')
            doi = attrs.get('doi')

            publications.append({
                "doi": doi,
                "publication_title": title,
                "pub_year": str(year),
                "status": "Ready (DataCite)",
            })
        return publications