"""
Lens.org API wrapper.

Implements `PublicationSource` using the Lens Scholarly API.
"""

from typing import Any, Dict, List, Optional

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.lens")


class LensAPI(BasePublicationAPI):
    """Searches Lens.org for scholarly works matching a query string."""

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
        self._source_name = "lens"
        self.base_url = "https://api.lens.org/scholarly/search"
        self.api_key = api_key

    @with_http_backoff(max_retries=3, base_delay=2.0)
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Lens.org for publications matching `query`.

        Args:
            query: Free‑text search query (supports Boolean syntax).
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status, lens_id
        """
        if not self.api_key:
            logger.debug("Lens API key missing; skipping")
            return []

        payload = {
            "query": {"query_string": f'"{query}"'},
            "size": limit,
            "sort": [{"year_published": "desc"}],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        self._rate_limit('lens')
        resp = self.session.post(
            self.base_url, json=payload, headers=headers, timeout=self.timeout
        )
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for hit in data.get('data', []):
            # Extract DOI from external_ids
            doi = None
            for ext_id in hit.get('external_ids', []):
                if ext_id.get('type') == 'doi':
                    doi = ext_id.get('value')
                    break

            publications.append({
                "doi": doi,
                "publication_title": hit.get('title', 'Unknown Title'),
                "pub_year": str(hit.get('year_published', 'N/A')),
                "status": "Ready (Lens.org)",
                "lens_id": hit.get('lens_id'),
            })
        return publications