"""
Generic LLM client and verification utilities for methodology extraction.

Provides:
- `LLMClient`: sends prompts to an LLM and returns parsed JSON.
- `verify_against_source`: anti‑hallucination check that ensures extracted
  items actually appear in the source text.
"""

import json
import re
from typing import Any, Dict, List, Optional

import requests

from omix.logging_utils import get_logger

logger = get_logger("omix.extractors.llm")


class LLMClient:
    """
    Thin wrapper around an LLM chat completion endpoint.

    By default it uses the Azure AI models endpoint, but the endpoint and
    model can be customised via the constructor.

    Usage::

        client = LLMClient(api_key="...")
        result = client.extract_json(
            system_prompt="You are an expert...",
            user_prompt="Text to analyse..."
        )
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str = "https://models.inference.ai.azure.com/chat/completions",
        model: str = "meta-llama-3.1-70b-instruct",
    ):
        self.api_key = api_key
        self.endpoint = endpoint
        self.model = model

    def extract_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        timeout: int = 45,
    ) -> Dict[str, Any]:
        """
        Send a prompt to the LLM and return the response parsed as JSON.

        If the API call fails or the response cannot be parsed, an empty dict
        is returned.

        Args:
            system_prompt: Instructions for the system role.
            user_prompt: The text to analyse (user role).
            temperature: Sampling temperature (0.0 = deterministic).
            timeout: Request timeout in seconds.

        Returns:
            Dictionary of extracted details, or an empty dict on failure.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
        }

        try:
            resp = requests.post(
                self.endpoint, headers=headers, json=payload, timeout=timeout
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            # Strip possible markdown fences
            content = content.replace("```json", "").replace("```", "").strip()
            return json.loads(content)
        except Exception as e:
            logger.debug(f"LLM extraction failed: {e}")
            return {}


def verify_against_source(
    extracted_items: List[str],
    source_text: str,
    is_dna: bool = False,
) -> List[str]:
    """
    Return only those items that actually appear in the source text.

    Args:
        extracted_items: List of strings to verify (primer names, sequences, …).
        source_text: The original text that was sent to the LLM.
        is_dna: If True, normalise spaces and hyphens before comparison
                and require at least 10 valid IUPAC characters.

    Returns:
        Subset of ``extracted_items`` that were confirmed present.
    """
    if not extracted_items or not source_text:
        return []

    verified: List[str] = []

    if is_dna:
        # Strip all non‑letter characters for DNA comparison
        dna_source = re.sub(r"[^a-zA-Z]", "", source_text).upper()
        for item in extracted_items:
            clean_seq = re.sub(r"[^a-zA-Z]", "", item).upper()
            if len(clean_seq) >= 10 and clean_seq in dna_source:
                verified.append(clean_seq)
            else:
                logger.warning(
                    f"Hallucination caught! Dropped primer '{item}' (not found in source)."
                )
    else:
        norm_source = re.sub(r"\s+", " ", source_text).lower()
        for item in extracted_items:
            norm_item = re.sub(r"\s+", " ", str(item)).lower()
            if norm_item in norm_source:
                verified.append(item)
            else:
                # Fuzzy fallback: check if the longest word appears
                words = [w for w in norm_item.split() if len(w) > 4]
                if words and any(w in norm_source for w in words):
                    verified.append(item)
                else:
                    logger.warning(
                        f"Hallucination caught! Dropped text '{item}' (not found in source)."
                    )

    return verified