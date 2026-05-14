"""
16S rRNA amplicon methodology extractor.

Implements `OmicsExtractor` with prompts tailored to 16S primer/region
extraction, anti‑hallucination cleaning, and optional validation against
a primer database (ProbeBaseDatabase).
"""

import re
from typing import Any, Dict, List, Optional

from ..llm import LLMClient, verify_against_source
from .base import OmicsExtractor
from omix.logging_utils import get_logger

logger = get_logger("omix.extractors.16s")


class SixteenSExtractor(OmicsExtractor):
    """
    16S‑specific methodology extractor.

    Uses an LLM to pull primer names, sequences, variable regions, kits,
    cycling conditions, etc. from text, then verifies the output against
    the source to prevent hallucinations. Optionally validates primers
    against a ProbeBaseDatabase (or any object with a
    ``validate_extracted_pair`` method).
    """

    # Expected JSON keys
    EXPECTED_KEYS = [
        "primer_names",
        "primer_sequences",
        "variable_regions",
        "extraction_protocol_and_kits",
        "pcr_conditions_and_kits",
        "sequencing_details",
        "sample_storage",
        "unextracted_flag",
        "unextracted_reason",
    ]

    def __init__(
        self,
        api_key: str,
        primer_db: Optional[Any] = None,   # ProbeBaseDatabase or any object with validate_extracted_pair
        llm_client: Optional[LLMClient] = None,
    ):
        """
        Args:
            api_key: LLM API key (set to empty string to disable LLM).
            primer_db: Optional database instance for primer validation.
            llm_client: Pre‑configured LLMClient; if None, creates one with the given api_key.
        """
        self.api_key = api_key
        self.primer_db = primer_db
        self.llm_client = llm_client or LLMClient(api_key)

    # ------------------------------------------------------------------
    #  OmicsExtractor interface
    # ------------------------------------------------------------------

    def get_llm_prompt(self, text_chunk: str) -> Dict[str, str]:
        """Return system and user prompts for 16S methodology extraction."""
        system_prompt = (
            "You are an expert bioinformatician. Read the provided methods text "
            "and extract the experimental details. Return ONLY a raw JSON object "
            "with absolutely no markdown formatting. "
            "CRITICAL: DO NOT GUESS OR INFER. EXTRACT EXACT STRINGS FROM THE TEXT. "
            "If something is not explicitly written, return an empty list. "
            "The JSON must contain exactly these keys:\n"
            "- 'sample_storage' (list of strings)\n"
            "- 'extraction_protocol_and_kits' (list of strings)\n"
            "- 'pcr_conditions_and_kits' (list of strings)\n"
            "- 'primer_names' (list of strings)\n"
            "- 'primer_sequences' (list of strings)\n"
            "- 'variable_regions' (list of strings)\n"
            "- 'sequencing_details' (list of strings)\n"
            "- 'unextracted_flag' (boolean): Set to true ONLY if the text "
            "explicitly references methodology that is MISSING from this text "
            "(e.g., 'primers are listed in Table S1').\n"
            "- 'unextracted_reason' (string): If unextracted_flag is true, "
            "briefly explain what is referenced. Else, empty string."
        )
        user_prompt = f"Text to analyze:\n{text_chunk[:20000]}"
        return {"system": system_prompt, "user": user_prompt}

    def get_expected_keys(self) -> List[str]:
        """Return the list of JSON keys the LLM must produce."""
        return self.EXPECTED_KEYS.copy()

    def post_process(
        self, llm_output: Dict[str, Any], source_text: str
    ) -> Dict[str, Any]:
        """
        Verify and clean LLM output against the source text.

        If the LLM returned nothing (e.g., because no API key), runs the
        regex DNA miner to at least capture sequences.
        """
        # If the LLM returned nothing, fall back to regex mining
        if not llm_output and source_text:
            mined = self._mine_dna_sequences(source_text)
            llm_output = {
                "primer_sequences": mined,
                "primer_names": [],
                "variable_regions": [],
                "extraction_protocol_and_kits": [],
                "pcr_conditions_and_kits": [],
                "sequencing_details": [],
                "sample_storage": [],
                "unextracted_flag": False,
                "unextracted_reason": "",
            }

        text_keys = [
            "sample_storage",
            "extraction_protocol_and_kits",
            "pcr_conditions_and_kits",
            "primer_names",
            "variable_regions",
            "sequencing_details",
        ]
        dna_keys = ["primer_sequences"]

        cleaned: Dict[str, Any] = {}
        for key in self.EXPECTED_KEYS:
            val = llm_output.get(key, [])
            if key in text_keys:
                if isinstance(val, list):
                    cleaned[key] = verify_against_source(val, source_text, is_dna=False)
                else:
                    cleaned[key] = []
            elif key in dna_keys:
                if isinstance(val, list):
                    cleaned[key] = verify_against_source(val, source_text, is_dna=True)
                else:
                    cleaned[key] = []
            else:
                # Boolean / string fields
                if key == "unextracted_flag":
                    cleaned[key] = bool(val) if val is not None else False
                elif key == "unextracted_reason":
                    cleaned[key] = str(val) if val else ""
                else:
                    cleaned[key] = val

        # Supplement with regex-mined sequences (in case LLM missed some)
        mined_seqs = self._mine_dna_sequences(source_text)
        if mined_seqs:
            existing = set(cleaned.get("primer_sequences", []))
            cleaned["primer_sequences"] = sorted(existing.union(mined_seqs))

        cleaned["verification_status"] = "Unverified"
        return cleaned

    def validate(self, extracted: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate extracted primer sequences against the primer database.

        Updates ``verification_status``, ``validated_primers``,
        ``variable_regions``, and ``primer_names`` if a match is found.
        Always sets ``primer_db_used`` to indicate whether a DB was available.
        """
        extracted["verification_status"] = "Unverified"
        extracted.setdefault("validated_primers", [])

        if self.primer_db is None:
            extracted["verification_status"] = "Unverified (no primer DB)"
            extracted["primer_db_used"] = False
            return extracted

        seqs = extracted.get("primer_sequences", [])
        if len(seqs) < 2:
            extracted["primer_db_used"] = True
            extracted["verification_status"] = "Unverified (insufficient sequences)"
            return extracted

        # Try both orientations
        try:
            payload = self.primer_db.validate_extracted_pair(seqs[0], seqs[1])
            if not payload:
                payload = self.primer_db.validate_extracted_pair(seqs[1], seqs[0])
            if payload:
                extracted["verification_status"] = "Verified (Coordinates)"
                extracted["validated_primers"] = [
                    payload.get("fwd_seq", seqs[0]),
                    payload.get("rev_seq", seqs[1]),
                ]
                extracted["variable_regions"] = [payload.get("region", "")]
                extracted["primer_names"] = [
                    payload.get("fwd_name", ""),
                    payload.get("rev_name", ""),
                ]
            else:
                extracted["verification_status"] = "Unverified (no DB match)"
        except Exception as e:
            logger.warning(f"Primer validation error: {e}")
            extracted["verification_status"] = "Unverified (validation error)"

        extracted["primer_db_used"] = True
        return extracted

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _mine_dna_sequences(text: str) -> List[str]:
        """Use the regex miner from cleaning to supplement LLM findings."""
        from ..cleaning import extract_dna_sequences
        return extract_dna_sequences(text)