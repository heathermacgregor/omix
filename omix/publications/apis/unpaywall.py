"""
Unpaywall API wrapper.

Implements `PublicationSource` to resolve DOIs to open‑access full‑text locations.
"""

import re
from typing import Any, Dict, List

from .base import BasePublicationAPI, with_http_backoff
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.unpaywall")


class UnpaywallAPI(BasePublicationAPI):
    """Resolves DOIs via Unpaywall to discover open‑access PDFs and metadata."""

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
        self._source_name = "unpaywall"
        self.base_url = "https://api.unpaywall.org/v2"

    @with_http_backoff()
    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Look up a DOI in Unpaywall.

        Args:
            query: A DOI string (must start with ``10.``).
            limit: Ignored – Unpaywall returns at most one record per DOI.

        Returns:
            A list with one publication dict if the DOI exists and has OA metadata,
            otherwise an empty list. The dict additionally contains ``pdf_url``
            if an open‑access PDF link is available.
        """
        doi = query.strip()
        if not re.match(r'^10\.\d{4,}/', doi):
            return []

        params = {"email": self.email}
        url = f"{self.base_url}/{doi}"

        self._rate_limit('unpaywall')
        resp = self.session.get(url, params=params, timeout=self.timeout)

        if resp.status_code == 404:
            logger.debug(f"DOI {doi} not found in Unpaywall")
            return []
        resp.raise_for_status()
        data = resp.json()

        # Extract title and year if available
        title = data.get('title', 'Unknown Title')
        year = data.get('year', 'N/A')

        # Best OA location
        pdf_url = None
        best_oa = data.get('best_oa_location')
        if best_oa and best_oa.get('url_for_pdf'):
            pdf_url = best_oa['url_for_pdf']
        else:
            for loc in data.get('oa_locations', []):
                if loc.get('url_for_pdf'):
                    pdf_url = loc['url_for_pdf']
                    break

        publication = {
            "doi": doi,
            "publication_title": title,
            "pub_year": str(year),
            "status": "Ready (Unpaywall)",
        }
        if pdf_url:
            publication["pdf_url"] = pdf_url
            publication["status"] = "Ready (Unpaywall – OA PDF available)"

        return [publication]