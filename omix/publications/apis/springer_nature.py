"""
Springer Nature API wrapper.

Implements `PublicationSource` using the Springer Nature Open Access API.
"""

from typing import Any, Dict, List, Optional

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.springer_nature")


class SpringerNatureAPI(BasePublicationAPI):
    """Searches Springer Nature for open‑access articles matching a query string."""

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
        self._source_name = "springer_nature"
        self.base_url = "http://api.springernature.com/openaccess/json"
        self.api_key = api_key

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Springer Nature for publications matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        if not self.api_key:
            logger.debug("Springer Nature API key missing; skipping")
            return []

        params = {
            "q": f'fulltext:"{query}"',
            "api_key": self.api_key,
            "p": limit,
        }
        self._rate_limit('springer_nature')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('records', []):
            year = str(item.get('publicationDate', 'N/A'))[:4]
            publications.append({
                "doi": item.get('doi'),
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": year,
                "status": "Ready (Springer Nature)",
            })
        return publications