"""
Text cleaning and extraction utilities for publication full‑text.

Functions:
- fix_spacing_in_text: normalise spacing and token separation.
- find_methods_section: isolate the Materials and Methods section.
- isolate_reference_section: locate the bibliography.
- find_citations_near_accession: detect citation clues near an accession mention.
- find_citation_entry_by_number: resolve a numbered citation to its full entry.
- extract_dna_sequences: mine DNA sequences from text (handles journal formatting).
- fetch_si_text: retrieve Supplementary Information from Europe PMC.
"""

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests

from omix.logging_utils import get_logger

logger = get_logger("omix.extractors.cleaning")


# --------------------------------------------------------------------------- #
#  Text normalisation
# --------------------------------------------------------------------------- #

def fix_spacing_in_text(text: str) -> str:
    """Insert spaces between run‑on tokens (e.g. 'word20°C' → 'word 20°C')."""
    text = re.sub(r'([a-z])([A-Z][a-z])', r'\1 \2', text)
    text = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text)
    text = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', text)
    text = re.sub(r'([.,;:])([a-zA-Z\d])', r'\1 \2', text)
    text = re.sub(r'([a-zA-Z])(-)([a-zA-Z])', r'\1 \2 \3', text)
    text = re.sub(r'([\]\)])([a-zA-Z\d\[])', r'\1 \2', text)
    return re.sub(r'\s+', ' ', text).strip()


# --------------------------------------------------------------------------- #
#  Section isolation
# --------------------------------------------------------------------------- #

_METHODS_START_HEADERS = [
    'materials and methods', 'methods and materials', 'methods',
    'experimental procedures', 'experimental section', 'research design',
    'experimental design', 'methodology', 'study design',
]

_METHODS_END_HEADERS = [
    'results', 'discussion', 'conclusions', 'acknowledgments', 'conclusion',
    'author contributions', 'references', 'supporting information',
    'data availability', 'competing interests', 'funding',
]


def find_methods_section(text: str) -> str:
    """
    Return the content of the Materials & Methods section.

    If no section is found, returns ``"Methods section not found in text."``
    so the caller can decide to fall back to the full text.
    """
    start_pos = -1
    start_header = ""
    for header in _METHODS_START_HEADERS:
        pattern = r'(?m)^[ \t]*' + re.escape(header) + r'[ \t]*[:\.\n\r]'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            pos = match.start()
            if start_pos == -1 or pos < start_pos:
                start_pos = pos
                start_header = match.group(0).strip()

    if start_pos == -1:
        return "Methods section not found in text."

    search_area = text[start_pos + len(start_header):]

    end_pos = -1
    for header in _METHODS_END_HEADERS:
        pattern = r'\n\s*' + re.escape(header) + r'\s*\n'
        match = re.search(pattern, search_area, re.IGNORECASE)
        if match:
            pos = match.start()
            if end_pos == -1 or pos < end_pos:
                end_pos = pos

    if end_pos != -1:
        section_text = search_area[:end_pos]
    else:
        section_text = search_area

    return (start_header + "\n" + section_text.strip()).strip()


def isolate_reference_section(full_text: str) -> str:
    """Return the reference / bibliography section of the paper."""
    ref_start_pattern = re.compile(
        r'\b(references|bibliography|works\s+cited|literature\s+cited)\b',
        re.IGNORECASE,
    )
    search_start_index = len(full_text) * 2 // 3
    match = ref_start_pattern.search(full_text, search_start_index)
    if match:
        return full_text[match.start():].strip()
    return ""


# --------------------------------------------------------------------------- #
#  Citation detection
# --------------------------------------------------------------------------- #

def find_citations_near_accession(
    full_text: str, accession: str, context_chars: int = 250
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Find citation clues (author‑year or numbered) near mentions of an accession.

    Returns a tuple of (list_of_clue_dicts, total_mentions).
    Each clue dict has keys: ``context_snippet``, ``citation_clues``.
    """
    normalized_text = re.sub(r'\s+', ' ', full_text)
    clean_accession = accession.replace(" ", "")
    search_text = re.sub(
        r'(\w)\s+(\d+)', r'\1\2', normalized_text, flags=re.IGNORECASE
    )

    citations_found: List[Dict[str, Any]] = []
    matches = list(re.finditer(re.escape(clean_accession), search_text, re.IGNORECASE))
    total_mentions = len(matches)

    for match in matches:
        # Look for citation clues in a window right after the match
        search_zone = search_text[
            max(0, match.start()) : min(len(search_text), match.end() + 40)
        ]

        # Author‑year pattern
        author_year_pattern = re.compile(
            r'\(?((?:[\w-]+\s?){1,3} et al\.?,? \d{4}|(?:[\w-]+\s?){1,3}, \d{4})\)?',
            re.IGNORECASE,
        )
        author_year_matches = author_year_pattern.findall(search_zone)

        # Numbered pattern
        numbered_pattern = re.compile(
            r'[\[\(]\s*(\d+)\s*(?:[–-]\s*\d+)?(?:\s*,\s*\d+)*\s*[\]\)]'
        )
        numbered_matches = numbered_pattern.findall(search_zone)

        clues = []
        if author_year_matches:
            clues.extend(
                f"Author-Year clue: {c.strip()}" for c in set(author_year_matches)
            )
        if numbered_matches:
            clues.extend(f"Numbered clue: {c}" for c in set(numbered_matches))

        if clues:
            context_snippet = search_text[
                max(0, match.start()) : min(len(search_text), match.end() + context_chars)
            ]
            citations_found.append({
                "context_snippet": context_snippet.strip(),
                "citation_clues": clues,
            })

    return citations_found, total_mentions


def find_citation_entry_by_number(
    reference_section: str, number: str
) -> Optional[str]:
    """
    Given a reference number (e.g. '12'), return the full citation entry
    from the reference section.
    """
    try:
        next_number = str(int(number) + 1)
    except ValueError:
        return None

    current_ref_start = (
        r'(\s*|^)(\[' + re.escape(number) + r'\]|\b' + re.escape(number) + r'\.)\s*'
    )
    next_ref_start = (
        r'(\s*|^)(\[' + re.escape(next_number)
        + r'\]|\b' + re.escape(next_number) + r'\.)\s*'
    )
    full_entry_pattern = re.compile(
        current_ref_start + r'(.*?)' + r'(?=' + next_ref_start + r'|$)',
        re.IGNORECASE | re.DOTALL,
    )
    match = full_entry_pattern.search(reference_section)
    if match:
        return f"{match.group(2).strip()} {match.group(3).strip()}"
    return None


# --------------------------------------------------------------------------- #
#  DNA sequence mining
# --------------------------------------------------------------------------- #

def extract_dna_sequences(text: str) -> List[str]:
    """
    Robustly mine DNA sequences from text.
    Strips all whitespace/dashes and finds all IUPAC substrings of length 15‑40.
    """
    iupac = set("ACGTUNRYSWKMBDHV")
    text = text.upper()
    # Remove spaces, dashes, prime symbols
    cleaned = re.sub(r'[\s\-‐′’]', '', text)

    runs = re.findall(r'[ACGTUNRYSWKMBDHV]+', cleaned)
    seqs: set[str] = set()
    for run in runs:
        for start in range(len(run) - 15 + 1):
            end = min(start + 40, len(run))
            sub = run[start:end]
            if 15 <= len(sub) <= 40 and all(c in iupac for c in sub):
                seqs.add(sub)
    return sorted(seqs, key=len)


# --------------------------------------------------------------------------- #
#  Supplementary Information (Europe PMC)
# --------------------------------------------------------------------------- #

def fetch_si_text(
    id_1: str,
    id_2: str = "",
    session: Any = None,
    timeout: int = 60,
    *args,
    **kwargs,
) -> str:
    """
    Attempt to fetch Supplementary Information using the Europe PMC REST API.

    Accepts multiple identifiers (e.g. PMID and DOI) and automatically uses
    the best one.  Returns the text content of supplementary sections, or an
    empty string if nothing is found.
    """
    request_session = session or requests.Session()
    identifier = str(id_1)
    if id_2 and '/' in str(id_2):
        identifier = str(id_2)
    elif '/' in str(id_1):
        identifier = str(id_1)

    if not identifier or identifier.lower() == 'nan':
        return ""

    si_content: List[str] = []

    try:
        # 1. Resolve identifier to PMCID
        query_str = (
            f'DOI:"{identifier}"' if '/' in identifier else f'EXT_ID:"{identifier}"'
        )
        search_params = {
            "query": query_str,
            "format": "json",
            "resultType": "core",
        }
        search_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        search_resp = request_session.get(
            search_url, params=search_params, timeout=timeout
        )
        search_resp.raise_for_status()
        results = search_resp.json().get("resultList", {}).get("result", [])
        if not results:
            return ""
        pmcid = results[0].get("pmcid")
        if not pmcid:
            return ""

        # 2. Fetch full‑text XML
        xml_url = (
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
        )
        xml_resp = request_session.get(xml_url, timeout=timeout)
        if xml_resp.status_code != 200:
            return ""

        # 3. Parse XML for supplementary sections
        root = ET.fromstring(xml_resp.content)
        for supp_node in root.findall('.//sec[@sec-type="supplementary-material"]'):
            text_pieces = [
                text.strip() for text in supp_node.itertext() if text.strip()
            ]
            if text_pieces:
                si_content.append(" ".join(text_pieces))
        for supp_node in root.findall('.//supplementary-material'):
            text_pieces = [
                text.strip() for text in supp_node.itertext() if text.strip()
            ]
            if text_pieces:
                si_content.append(" ".join(text_pieces))

    except requests.exceptions.RequestException as e:
        logger.warning(f"Network error fetching SI for {identifier}: {e}")
    except ET.ParseError as e:
        logger.warning(f"XML parse error for {identifier}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching SI for {identifier}: {e}")

    unique_si = list(dict.fromkeys(si_content))
    final_text = "\n\n".join(unique_si)

    # Detect placeholder links (e.g. "click here for Table S1")
    if len(final_text) < 100 and "click here" in final_text.lower():
        logger.warning(f"SI content for {identifier} appears to be a link placeholder")
        return ""

    return final_text