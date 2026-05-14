"""
NCBI Entrez API wrapper.

Implements `PublicationSource` using NCBI E‑utilities to find publications
linked to a BioProject or other accession.
"""

from typing import Any, Dict, List, Optional

from .base import BasePublicationAPI, with_http_backoff
from omix.publications.exceptions import InvalidAPIKeyError
from omix.logging_utils import get_logger

logger = get_logger("omix.apis.ncbi")


class NCBIAPI(BasePublicationAPI):
    """Searches NCBI for publications linked to an accession."""

    def __init__(
        self,
        email: str,
        api_key: Optional[str] = None,
        # FEATURE 3: Accept retry config
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
        self._source_name = "ncbi"
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.api_key = api_key

    def check_key_error(self, response, api_name):
        """NCBI returns a 400 with a JSON error when the API key is bad."""
        if response.status_code == 400:
            try:
                data = response.json()
                if data.get("error") == "API key invalid":
                    raise InvalidAPIKeyError(
                        api_name,
                        "Your NCBI API key is invalid. Remove it from your config or obtain a valid key "
                        "from https://www.ncbi.nlm.nih.gov/account/"
                    )
            except ValueError:
                pass  # not JSON, let normal error handler deal with it
        # also check generic 401/403
        super().check_key_error(response, api_name)

    @with_http_backoff()
    def _esearch(self, db: str, term: str) -> List[str]:
        """Run an esearch query and return list of UIDs."""
        params = {
            "db": db,
            "term": term,
            "retmode": "json",
            "tool": "omix",
            "email": self.email,
        }
        if self.api_key and self.api_key.strip():
            params["api_key"] = self.api_key
        self._rate_limit('ncbi')
        resp = self.session.get(
            f"{self.base_url}/esearch.fcgi", params=params, timeout=self.timeout
        )
        self.check_key_error(resp, self.source_name)
        resp.raise_for_status()
        data = resp.json()
        return data.get("esearchresult", {}).get("idlist", [])

    @with_http_backoff()
    def _esummary(self, db: str, uids: List[str]) -> List[Dict[str, Any]]:
        """Fetch summaries for a list of UIDs."""
        params = {
            "db": db,
            "id": ",".join(uids),
            "retmode": "json",
            "tool": "omix",
            "email": self.email,
        }
        if self.api_key and self.api_key.strip():
            params["api_key"] = self.api_key
        self._rate_limit('ncbi')
        resp = self.session.get(
            f"{self.base_url}/esummary.fcgi", params=params, timeout=self.timeout
        )
        self.check_key_error(resp, self.source_name)
        resp.raise_for_status()
        data = resp.json()

        # NCBI esummary JSON structure:
        # {"result": {"uids": ["12345","67890"], "12345": {...}, "67890": {...}}}
        result = data.get("result", {})
        uid_list = result.get("uids", [])
        articles: List[Dict[str, Any]] = []
        for uid in uid_list:
            article = result.get(uid)
            if isinstance(article, dict):
                articles.append(article)
        return articles

    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Search NCBI PubMed for publications linked to an accession.

        Strategy:
        1. Search bioproject → elink to pubmed → esummary.
        2. Fallback: direct PubMed search for the accession.
        """
        publications: List[Dict[str, Any]] = []

        # Step 1: try bioproject → pubmed link
        uids = self._esearch("bioproject", query)
        if uids:
            bioproject_uid = uids[0]
            # Link from bioproject to pubmed
            params = {
                "dbfrom": "bioproject",
                "db": "pubmed",
                "id": bioproject_uid,
                "retmode": "json",
                "tool": "omix",
                "email": self.email,
            }
            if self.api_key and self.api_key.strip():
                params["api_key"] = self.api_key
            self._rate_limit('ncbi')
            resp = self.session.get(
                f"{self.base_url}/elink.fcgi", params=params, timeout=self.timeout
            )
            self.check_key_error(resp, self.source_name)
            resp.raise_for_status()
            link_data = resp.json()
            pmids: List[str] = []
            for linkset in link_data.get("linksets", []):
                for linkdb in linkset.get("linksetdbs", []):
                    if linkdb.get("dbto") == "pubmed":
                        # links is a list of strings (pmids), not dicts
                        for pmid in linkdb.get("links", []):
                            if isinstance(pmid, str):
                                pmids.append(pmid)

            if pmids:
                pmids = pmids[:limit]
                summaries = self._esummary("pubmed", pmids)
                for article in summaries:
                    doi = ""
                    for aid in article.get("articleids", []):
                        if aid.get("idtype") == "doi":
                            doi = aid.get("value")
                            break
                    pub_date = article.get("pubdate", "")
                    year = pub_date.split()[0] if pub_date else "N/A"
                    publications.append({
                        "doi": doi,
                        "publication_title": article.get("title", "Unknown Title"),
                        "pub_year": str(year),
                        "status": "Ready (NCBI)",
                    })
                return publications

        # Step 2: fallback – direct PubMed search
        uids = self._esearch("pubmed", f"{query}[Accession]")
        if uids:
            uids = uids[:limit]
            summaries = self._esummary("pubmed", uids)
            for article in summaries:
                doi = ""
                for aid in article.get("articleids", []):
                    if aid.get("idtype") == "doi":
                        doi = aid.get("value")
                        break
                pub_date = article.get("pubdate", "")
                year = pub_date.split()[0] if pub_date else "N/A"
                publications.append({
                    "doi": doi,
                    "publication_title": article.get("title", "Unknown Title"),
                    "pub_year": str(year),
                    "status": "Ready (NCBI)",
                })

        return publications
    
    
class PMIDSource(NCBIAPI):
    """Fetch a single paper by PubMed ID."""

    def __init__(
        self,
        email: str,
        pmid: str,
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
        self._source_name = "pmid"
        self.pmid = pmid

    def search(self, query: str = "", limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        summaries = self._esummary("pubmed", [self.pmid])
        publications = []
        for article in summaries:
            doi = ""
            for aid in article.get("articleids", []):
                if aid.get("idtype") == "doi":
                    doi = aid.get("value")
                    break
            pub_date = article.get("pubdate", "")
            year = pub_date.split()[0] if pub_date else "N/A"
            publications.append({
                "doi": doi,
                "publication_title": article.get("title", "Unknown Title"),
                "pub_year": str(year),
                "status": "Ready (PMID)",
            })
        return publications