"""
Abstract base classes for publication sources and omics‑specific extractors.

All publication APIs must implement `PublicationSource`.
All methodology extractors must implement `OmicsExtractor`.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class PublicationSource(ABC):
    """Interface for a publication search API (Crossref, Europe PMC, NCBI, …)."""

    @abstractmethod
    def search(self, query: str, limit: int = 10, **kwargs: Any) -> Any:
        """
        Search for publications matching `query`.

        Args:
            query: The search string (e.g., an accession, title, or DOI).
            limit: Maximum number of results to return.
            **kwargs: Additional API‑specific parameters.

        Returns:
            A list of publication metadata dicts. Each dict must contain at least:
            - 'doi' (str or None)
            - 'publication_title' (str)
            - 'pub_year' (str or int)
            - 'status' (str) - a short description of the result source
        """
        ...

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short name of the source (e.g. 'crossref', 'europepmc')."""
        ...


class OmicsExtractor(ABC):
    """
    Interface for omics‑specific methodology extraction from publication text.

    Subclasses define:
    - The LLM prompt to use.
    - How to post‑process and verify LLM output against the source text.
    - How to validate extracted details against reference databases.
    """

    @abstractmethod
    def get_llm_prompt(self, text_chunk: str) -> Dict[str, str]:
        """Return the system and user prompts for the LLM."""
        ...

    @abstractmethod
    def get_expected_keys(self) -> List[str]:
        """List of JSON keys the LLM must return."""
        ...

    @abstractmethod
    def post_process(
        self, llm_output: Dict[str, Any], source_text: str
    ) -> Dict[str, Any]:
        """
        Verify and clean the LLM output against the original text.

        Args:
            llm_output: Raw parsed JSON from the LLM.
            source_text: The exact text that was sent to the LLM.

        Returns:
            Verified and cleaned dictionary with the same keys as `get_expected_keys`.
        """
        ...

    @abstractmethod
    def validate(self, extracted: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate extracted details against reference databases.

        Args:
            extracted: The verified dictionary from `post_process`.

        Returns:
            The same dictionary, possibly augmented with a 'validation_status' key
            and corrected values.
        """
        ...