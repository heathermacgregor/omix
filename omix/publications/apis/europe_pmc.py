"""
Europe PMC API wrapper.

Implements `PublicationSource` using the Europe PMC REST API.
"""

from typing import Any, Dict, List, Optional

from omix.logging_utils import get_logger
from .base import BasePublicationAPI, with_http_backoff

logger = get_logger("omix.apis.europepmc")


class EuropePMCAPI(BasePublicationAPI):
    """Searches Europe PMC for publications linked to an accession string."""

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
        self._source_name = "europepmc"
        self.base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Europe PMC for publications matching the query.

        Args:
            query: Free‑text search query (e.g., accession, title, DATA prefix).
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "query": query,
            "resultType": "lite",
            "format": "json",
            "pageSize": limit,
        }
        self._rate_limit('europepmc')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for item in data.get('resultList', {}).get('result', []):
            publications.append({
                "doi": item.get('doi'),
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": str(item.get('pubYear', 'N/A')),
                "status": "Ready (Europe PMC)",
            })
        return publications