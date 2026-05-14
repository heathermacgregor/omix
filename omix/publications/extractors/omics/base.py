"""
Abstract base class for omics‑specific methodology extractors.

Every omics type (16S, metagenomics, metatranscriptomics, etc.) must
subclass `OmicsExtractor` and implement:

- `get_llm_prompt()` – build the LLM prompt.
- `get_expected_keys()` – list of JSON keys the LLM should return.
- `post_process()` – verify and clean LLM output against the source text.
- `validate()` – cross‑check extracted details against reference databases.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class OmicsExtractor(ABC):
    """
    Interface for extracting methodology details from publication text.

    Subclasses encapsulate all omics‑specific knowledge:
    - Which experimental details to ask for (primers, kits, platforms).
    - How to verify the LLM output against the source text.
    - How to validate the extracted information against known reference databases.
    """

    @abstractmethod
    def get_llm_prompt(self, text_chunk: str) -> Dict[str, str]:
        """
        Return the system and user prompts for the LLM.

        Args:
            text_chunk: The text to analyse (methods section, full text, or SI).

        Returns:
            A dictionary with keys ``"system"`` and ``"user"``.
        """
        ...

    @abstractmethod
    def get_expected_keys(self) -> List[str]:
        """
        Return the list of JSON keys the LLM is instructed to produce.

        Example for 16S: ``["primer_names", "primer_sequences", "variable_regions", …]``
        """
        ...

    @abstractmethod
    def post_process(
        self, llm_output: Dict[str, Any], source_text: str
    ) -> Dict[str, Any]:
        """
        Verify and clean the LLM output against the original source text.

        Must:
        - Run ``verify_against_source`` on text and DNA fields.
        - Ensure all expected keys are present (empty list if missing).
        - Set an ``unextracted_flag`` if the text references missing data.

        Args:
            llm_output: Raw parsed JSON from the LLM.
            source_text: The exact text that was sent to the LLM.

        Returns:
            Cleaned dictionary with the same keys as ``get_expected_keys``
            plus a ``verification_status`` key.
        """
        ...

    @abstractmethod
    def validate(self, extracted: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate extracted details against reference databases.

        For 16S this would check primer sequences against a primer database.
        For metagenomics it might check tool names against a curated list.

        Args:
            extracted: The verified dictionary from ``post_process``.

        Returns:
            The dictionary, possibly augmented with validated fields
            (e.g. ``validated_primers``, ``variable_regions``) and a
            ``verification_status`` key.
        """
        ...