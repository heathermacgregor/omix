"""
Primer database for 16S rRNA validation.

Supports two backends:
1. ProbeBase SQLite database (built via `omix build-primer-db`)
2. Built-in CSV of common 16S primer pairs (always available)

Falls back to the built-in database when no probeBase DB is available.
"""

import csv
import io
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from omix.logging_utils import get_logger

logger = get_logger("omix.validators.primer_db")

# IUPAC nucleotide ambiguity codes
IUPAC_MAP = {
    'R': 'AG', 'Y': 'CT', 'S': 'GC', 'W': 'AT', 'K': 'GT',
    'M': 'AC', 'B': 'CGT', 'D': 'AGT', 'H': 'ACT', 'V': 'ACG',
    'N': 'ACGT', '-': '',
}

# Built‑in fallback: common 16S primer pairs from the literature
# Columns: name,sequence,direction,target,position
_BUILTIN_PRIMERS_CSV = """\
name,sequence,direction,target,position
27F,AGAGTTTGATCMTGGCTCAG,Forward primer,16S V1-V3,8-27
338F,ACTCCTACGGGAGGCAGCAG,Forward primer,16S V3,338-355
341F,CCTACGGGNGGCWGCAG,Forward primer,16S V3-V4,341-357
515F,GTGYCAGCMGCCGCGGTAA,Forward primer,16S V4,515-533
515F_original,GTGCCAGCMGCCGCGG,Forward primer,16S V4,515-530
806R,GGACTACHVGGGTWTCTAAT,Reverse primer,16S V4,806-787
806R_original,GGACTACVSGGGTATCTAAT,Reverse primer,16S V4,806-788
907R,CCGTCAATTCMTTTRAGTTT,Reverse primer,16S V5-V6,907-888
926F,AAACTYAAAKGAATTGACGG,Forward primer,16S V6-V8,926-945
1392R,ACGGGCGGTGTGTRC,Reverse primer,16S V8,1392-1378
1492R,GGTTACCTTGTTACGACTT,Reverse primer,16S V9,1492-1474
337F,GACTCCTACGGGAGGCWGCAG,Forward primer,16S V3,337-357
518R,ATTACCGCGGCTGCTGG,Reverse primer,16S V4,518-501
785F,GGATTAGATACCCTGGTA,Forward primer,16S V5,785-803
805R,GACTACHVGGGTATCTAATCC,Reverse primer,16S V3-V4,805-785
1100F,YAACGAGCGCAACCC,Forward primer,16S V7,1100-1114
1100R,GGGTTGCGCTCGTTG,Reverse primer,16S V7,1100-1086
928F,TAAAACTYAAAKGAATTGACGGG,Forward primer,16S V6-V8,928-950
336R,ACTGCTGCCTCCCGTAGGAGT,Reverse primer,16S V3,336-317
"""


class ProbeBaseDatabase:
    """probeBase‑derived primer pair validator with IUPAC fuzzy matching."""

    def __init__(self, db_path: Optional[Path] = None, use_builtin: bool = False):
        """
        Args:
            db_path: Path to a probeBase SQLite database. If None or file missing,
                     falls back to the built‑in primer list.
            use_builtin: If True, skip the probeBase DB entirely and use built‑in.
        """
        self.records: List[Dict[str, Any]] = []

        if use_builtin or db_path is None or not db_path.exists():
            self._load_builtin()
        else:
            try:
                self._load_sqlite(db_path)
            except Exception as e:
                logger.warning(f"Failed to load probeBase DB ({e}), falling back to built‑in.")
                self._load_builtin()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_sqlite(self, db_path: Path) -> None:
        """Load primers from a probeBase SQLite database."""
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM primers").fetchall()
        self.records = [dict(r) for r in rows]
        logger.info(f"Loaded {len(self.records)} primers from probeBase DB.")

    def _load_builtin(self) -> None:
        """Load the built‑in common 16S primer list."""
        reader = csv.DictReader(io.StringIO(_BUILTIN_PRIMERS_CSV))
        self.records = list(reader)
        logger.info(f"Loaded {len(self.records)} primers from built‑in database.")

    # ------------------------------------------------------------------
    # IUPAC matching
    # ------------------------------------------------------------------

    @staticmethod
    def _iupac_match(primer_seq: str, db_seq: str) -> bool:
        """Return True if primer_seq matches db_seq, respecting IUPAC codes."""
        if len(primer_seq) != len(db_seq):
            return False
        for p, d in zip(primer_seq.upper(), db_seq.upper()):
            if p == d:
                continue
            allowed_db = IUPAC_MAP.get(d, d)
            allowed_primer = IUPAC_MAP.get(p, p)
            if p not in allowed_db and d not in allowed_primer:
                return False
        return True

    def _find_matching_records(self, sequence: str) -> List[Dict[str, Any]]:
        """Return all records whose Sequence IUPAC‑matches the given sequence."""
        return [
            r for r in self.records
            if r.get('sequence') and self._iupac_match(sequence, r['sequence'].upper())
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_extracted_pair(
        self, seq1: str, seq2: str
    ) -> Optional[Dict[str, Any]]:
        """Validate a primer pair against the database.

        Returns a dictionary containing the validated forward and reverse
        primer sequences, the matched region, and the primer names, or None if
        no valid pair is found.
        """
        matches1 = self._find_matching_records(seq1)
        matches2 = self._find_matching_records(seq2)

        def _get_fwd(records): return next(
            (r for r in records if 'forward' in (r.get('direction') or '').lower()), None
        )
        def _get_rev(records): return next(
            (r for r in records if 'reverse' in (r.get('direction') or '').lower()), None
        )

        # Try original orientation
        fwd, rev = _get_fwd(matches1), _get_rev(matches2)
        if fwd and rev:
            return self._build_result(fwd, rev)
        # Swapped
        fwd, rev = _get_fwd(matches2), _get_rev(matches1)
        if fwd and rev:
            return self._build_result(fwd, rev)

        # Relaxed: any two distinct records with opposite directions
        fwds = [r for r in matches1 if 'forward' in (r.get('direction') or '').lower()]
        revs = [r for r in matches2 if 'reverse' in (r.get('direction') or '').lower()]
        if fwds and revs:
            return self._build_result(fwds[0], revs[0])
        fwds = [r for r in matches2 if 'forward' in (r.get('direction') or '').lower()]
        revs = [r for r in matches1 if 'reverse' in (r.get('direction') or '').lower()]
        if fwds and revs:
            return self._build_result(fwds[0], revs[0])

        return None

    @staticmethod
    def _build_result(fwd: Dict, rev: Dict) -> Dict[str, Any]:
        region = fwd.get('target') or rev.get('target') or ''
        return {
            "fwd_seq": fwd.get('sequence', '').upper(),
            "rev_seq": rev.get('sequence', '').upper(),
            "region": region,
            "fwd_name": fwd.get('name', ''),
            "rev_name": rev.get('name', ''),
        }