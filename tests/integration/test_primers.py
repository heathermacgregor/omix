"""Primer mining regression tests."""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET

import pytest
import requests

from omix.publications.extractors.cleaning import fix_spacing_in_text
from omix.validators.primer_db import ProbeBaseDatabase


def extract_dna_sequences(text: str) -> list[str]:
    """Find continuous IUPAC-only substrings between 15 and 40 bases long."""
    iupac = set("ACGTUNRYSWKMBDHV")
    cleaned = re.sub(r"[\s\-]+", "", text.upper())
    runs = re.findall(r"[ACGTUNRYSWKMBDHV]+", cleaned)
    seqs = set()
    for run in runs:
        for start in range(len(run) - 15 + 1):
            end = min(start + 40, len(run))
            sub = run[start:end]
            if len(sub) >= 15 and all(c in iupac for c in sub):
                seqs.add(sub)
    return sorted(seqs, key=len)


def test_extract_dna_sequences_sanity():
    test_str = "Primers used: GTGCCAGCMGCCGCGGTAA, GGACTACHVGGGTWTCTAAT."
    test_seqs = extract_dna_sequences(test_str)

    assert "GTGCCAGCMGCCGCGGTAA" in test_seqs
    assert "GGACTACHVGGGTWTCTAAT" in test_seqs


@pytest.mark.integration
def test_real_article_primer_mining():
    if os.getenv("OMIX_RUN_INTEGRATION") != "1":
        pytest.skip("Set OMIX_RUN_INTEGRATION=1 to run primer integration tests")

    pmcid = "PMC6011224"
    xml_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"

    resp = requests.get(xml_url, timeout=30)
    assert resp.status_code == 200

    root = ET.fromstring(resp.text)
    clean_text = fix_spacing_in_text(" ".join(root.itertext()))

    mined = extract_dna_sequences(clean_text)
    assert mined

    db = ProbeBaseDatabase(use_builtin=True)
    matches = []
    for i in range(len(mined)):
        for j in range(i + 1, len(mined)):
            match = db.validate_extracted_pair(mined[i], mined[j])
            if match:
                matches.append(match)

    assert isinstance(matches, list)