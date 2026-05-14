"""
Zenodo API wrapper.

Implements `PublicationSource` using the Zenodo REST API to search for
datasets, software, and publications linked to an accession.
"""

from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.zenodo")


class ZenodoAPI(BasePublicationAPI):
    """Searches Zenodo for records matching a query string."""

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
        self._source_name = "zenodo"
        self.base_url = "https://zenodo.org/api/records"

    @with_http_backoff(max_retries=3, base_delay=2.0)
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search Zenodo for records matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
            - zenodo_id, supplementary_content (optional)
        """
        params = {
            "q": f'"{query}"',
            "size": limit,
        }
        self._rate_limit('zenodo')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        publications: List[Dict[str, Any]] = []
        for hit in data.get('hits', {}).get('hits', []):
            metadata = hit.get('metadata', {})
            record_id = hit.get('id')

            # Discover supplementary files (spreadsheets, primers, mappings)
            supplementary_text = self._discover_supplementary_content(record_id)

            publications.append({
                "doi": hit.get('doi') or metadata.get('doi'),
                "publication_title": metadata.get('title', 'Untitled Zenodo Record'),
                "pub_year": str(metadata.get('publication_date', 'N/A'))[:4],
                "status": "Ready (Zenodo)",
                "zenodo_id": record_id,
                "supplementary_content": supplementary_text,
            })
        return publications

    def _discover_supplementary_content(self, record_id: str) -> str:
        """Scan Zenodo file manifest for supplementary content (primers, mappings, etc.)."""
        discovered = ""
        try:
            files_url = f"https://zenodo.org/api/records/{record_id}/files"
            resp = self.session.get(files_url, timeout=10)
            if resp.status_code == 200:
                files = resp.json()
                for f in files:
                    fname = f.get('key', '').lower()
                    if any(k in fname for k in ('primer', 'mapping', 'metadata', 'methods', 's1', 'supplement')):
                        discovered += (
                            f" [File Found: {f.get('key')} - "
                            f"{f.get('links', {}).get('self', '')}]"
                        )
        except Exception as e:
            logger.debug(f"Zenodo file discovery failed for {record_id}: {e}")
        return discovered