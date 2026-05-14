"""
ArXiv API wrapper.

Implements `PublicationSource` using the ArXiv API (Atom XML).
"""

import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.arxiv")


class ArxivAPI(BasePublicationAPI):
    """Searches ArXiv for preprints matching a query string."""

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
        self._source_name = "arxiv"
        self.base_url = "http://export.arxiv.org/api/query"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search ArXiv for preprints matching `query`.

        Args:
            query: Free‑text search query.
            limit: Maximum number of results to return.

        Returns:
            List of publication dicts with keys:
            - doi, publication_title, pub_year, status
        """
        params = {
            "search_query": f'all:"{query}"',
            "sortBy": "submittedDate",
            "sortOrder": "ascending",
            "max_results": limit,
        }
        self._rate_limit('arxiv')
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {'a': 'http://www.w3.org/2005/Atom'}

        publications: List[Dict[str, Any]] = []
        for entry in root.findall('a:entry', ns):
            title_elem = entry.find('a:title', ns)
            title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Unknown Title"

            published_elem = entry.find('a:published', ns)
            year = published_elem.text[:4] if published_elem is not None and published_elem.text else "N/A"

            doi = None
            for link in entry.findall('a:link', ns):
                if link.attrib.get('title') == 'doi':
                    href = link.attrib.get('href', '')
                    doi = href.split('doi.org/')[-1] if 'doi.org/' in href else None
                    break

            publications.append({
                "doi": doi,
                "publication_title": title,
                "pub_year": str(year),
                "status": "Ready (ArXiv)",
            })
        return publications