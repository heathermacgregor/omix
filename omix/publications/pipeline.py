"""Shared helpers for publication validation and dataframe integration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


def _publication_doi(publication: Dict) -> Optional[str]:
    doi = publication.get("doi") or publication.get("publication_doi")
    if doi:
        return str(doi).strip()

    external_ids = publication.get("externalIds")
    if isinstance(external_ids, dict):
        doi = external_ids.get("DOI")
        if doi:
            return str(doi).strip()

    return None


def filter_publications_by_accession(
    publications: Dict[str, List[Dict]],
    validated_file: Optional[Path] = None,
) -> Dict[str, List[Dict]]:
    """Keep publications that directly mention the study accession."""
    if validated_file and validated_file.exists():
        with validated_file.open() as handle:
            return json.load(handle)

    filtered: Dict[str, List[Dict]] = {}
    for study_accession, pubs in publications.items():
        kept: List[Dict] = []
        for pub in pubs:
            if pub.get("status") != "✓ Extraction complete.":
                continue

            matched_queries = pub.get("matched_queries", [])
            accession_in_text = pub.get("accession_mentions_in_text", 0) > 0
            direct_match = any(
                query == study_accession
                or (study_accession in str(query) and not str(query).startswith("DATA:"))
                for query in matched_queries
            )

            if direct_match or accession_in_text:
                kept.append(pub)

        filtered[study_accession] = kept

    return filtered


def populate_publication_fields(
    df: pd.DataFrame,
    publications: Dict[str, Iterable[Dict]],
) -> Tuple[int, int]:
    """Populate publication_count and publication_dois columns."""
    filled_rows = 0
    studies_with_dois = 0

    if "publication_count" not in df.columns:
        df["publication_count"] = None
    if "publication_dois" not in df.columns:
        df["publication_dois"] = None
    if "study_accession" not in df.columns:
        return filled_rows, studies_with_dois

    for study_accession, pubs in publications.items():
        dois: List[str] = []
        seen = set()
        for pub in pubs:
            if not isinstance(pub, dict):
                continue
            doi = _publication_doi(pub)
            if doi and doi not in seen:
                dois.append(doi)
                seen.add(doi)

        mask = df["study_accession"] == study_accession
        if not mask.any():
            continue

        df.loc[mask, "publication_count"] = len(dois)
        df.loc[mask, "publication_dois"] = "; ".join(dois) if dois else ""
        filled_rows += int(mask.sum())
        if dois:
            studies_with_dois += 1

    return filled_rows, studies_with_dois