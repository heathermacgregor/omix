"""
Semantic Scholar API wrapper.

Implements `PublicationSource` using the Semantic Scholar Academic Graph API.
"""

import time
from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.semantic_scholar")


class SemanticScholarAPI(BasePublicationAPI):
    """Searches Semantic Scholar for publications matching a query string."""

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
        self._source_name = "semantic_scholar"
        self.base_url = "https://api.semanticscholar.org/graph/v1/paper/search"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Semantic Scholar for publications matching `query`.
        Free tier: 1 req/s.  Pre‑emptively delay to avoid 429 storms.
        """
        # Pre‑emptive delay – free tier allows 1 req/s
        self._rate_limit('semantic_scholar')

        params = {
            "query": query,
            "fields": "title,year,externalIds",
            "limit": min(limit, 100),
        }

        try:
            resp = self.session.get(self.base_url, params=params, timeout=self.timeout)

            # Honour Retry‑After header if rate‑limited
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning(f"Semantic Scholar 429 – waiting {retry_after}s")
                time.sleep(retry_after)
                resp = self.session.get(self.base_url, params=params, timeout=self.timeout)

            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"Semantic Scholar request failed: {e}")
            return []

        publications: List[Dict[str, Any]] = []
        for item in data.get('data', []):
            doi = None
            ext_ids = item.get('externalIds', {})
            if isinstance(ext_ids, dict):
                doi = ext_ids.get('DOI')
            publications.append({
                "doi": doi,
                "publication_title": item.get('title', 'Unknown Title'),
                "pub_year": str(item.get('year', 'N/A')),
                "status": "Ready (Semantic Scholar)",
            })
        return publications