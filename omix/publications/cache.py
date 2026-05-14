"""
SQLite cache for publication search results and DOI metadata.

Tables:
- publication_cache: stores full JSON results keyed by bioproject/study accession.
- doi_metadata_cache: stores resolved DOI metadata.
- failed_lookups: records failed queries to avoid repeated attempts.
"""

import json
import sqlite3
import time
from pathlib import Path
from typing import Optional

from omix.logging_utils import get_logger

logger = get_logger("omix.publications.cache")


# --------------------------------------------------------------------------- #
#  Table creation
# --------------------------------------------------------------------------- #

def create_cache_tables(db_path: Path) -> None:
    """Create the required cache tables if they don't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS publication_cache (
                bioproject_id TEXT PRIMARY KEY,
                results_json TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                source_api TEXT,
                success_count INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS doi_metadata_cache (
                doi TEXT PRIMARY KEY,
                metadata_json TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                citation_count INTEGER,
                full_text_available BOOLEAN
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS failed_lookups (
                accession TEXT PRIMARY KEY,
                attempted_apis TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                retry_after REAL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pub_timestamp
            ON publication_cache(timestamp)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_doi_timestamp
            ON doi_metadata_cache(timestamp)
        """)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")


# --------------------------------------------------------------------------- #
#  Cache manager
# --------------------------------------------------------------------------- #

class PublicationCache:
    """Manages publication and DOI metadata caches."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        create_cache_tables(db_path)

    # --- Publication results ------------------------------------------------ #

    def get(self, bioproject_id: str) -> Optional[list]:
        """Return cached publication results for a BioProject/study accession."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                row = conn.execute(
                    "SELECT results_json FROM publication_cache WHERE bioproject_id = ?",
                    (bioproject_id,),
                ).fetchone()
                if row:
                    return json.loads(row[0])
        except sqlite3.Error as e:
            logger.error(f"Publication cache read failed for '{bioproject_id}': {e}")
        return None

    def set(self, bioproject_id: str, results: list) -> None:
        """Store publication results for a BioProject/study accession."""
        try:
            results_json = json.dumps(results)
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO publication_cache (bioproject_id, results_json) VALUES (?, ?)",
                    (bioproject_id, results_json),
                )
        except sqlite3.Error as e:
            logger.error(f"Publication cache write failed for '{bioproject_id}': {e}")

    # --- DOI metadata ------------------------------------------------------- #

    def get_doi_metadata(self, doi: str) -> Optional[dict]:
        """Return cached metadata for a DOI."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                row = conn.execute(
                    "SELECT metadata_json FROM doi_metadata_cache WHERE doi = ?",
                    (doi,),
                ).fetchone()
                if row:
                    return json.loads(row[0])
        except sqlite3.Error as e:
            logger.warning(f"DOI cache read failed for '{doi}': {e}")
        return None

    def set_doi_metadata(
        self,
        doi: str,
        metadata: dict,
        citation_count: Optional[int] = None,
        full_text_available: Optional[bool] = None,
    ) -> None:
        """Cache metadata for a DOI."""
        try:
            metadata_json = json.dumps(metadata)
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO doi_metadata_cache (doi, metadata_json, citation_count, full_text_available) VALUES (?, ?, ?, ?)",
                    (doi, metadata_json, citation_count, full_text_available),
                )
        except sqlite3.Error as e:
            logger.warning(f"DOI cache write failed for '{doi}': {e}")

    # --- Failed lookups ----------------------------------------------------- #

    def is_failed(self, accession: str) -> bool:
        """Check if a recent failed lookup exists for the accession."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                row = conn.execute(
                    "SELECT retry_after FROM failed_lookups WHERE accession = ?",
                    (accession,),
                ).fetchone()
                if row and row[0]:
                    return time.time() < row[0]
        except sqlite3.Error:
            pass
        return False

    def mark_failed(self, accession: str, attempted_apis: str, retry_after_seconds: float = 3600.0) -> None:
        """Record a failed lookup to avoid immediate retries."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO failed_lookups (accession, attempted_apis, retry_after) VALUES (?, ?, ?)",
                    (accession, attempted_apis, time.time() + retry_after_seconds),
                )
        except sqlite3.Error as e:
            logger.warning(f"Failed lookup cache write failed for '{accession}': {e}")

    # --- Cleanup ------------------------------------------------------------ #

    def cleanup(self, max_size_mb: float = 500.0, target_size_mb: float = 400.0) -> int:
        """
        Remove oldest entries to keep the cache file under `max_size_mb`.

        Returns:
            Number of entries deleted.
        """
        if not self.db_path.exists():
            return 0

        cache_size_mb = self.db_path.stat().st_size / 1e6
        if cache_size_mb <= max_size_mb:
            return 0

        deleted = 0
        try:
            with sqlite3.connect(str(self.db_path), timeout=10.0) as conn:
                # LRU eviction: delete oldest entries until size is acceptable
                while True:
                    row = conn.execute(
                        "SELECT bioproject_id FROM publication_cache ORDER BY timestamp ASC LIMIT 1"
                    ).fetchone()
                    if not row:
                        break
                    conn.execute(
                        "DELETE FROM publication_cache WHERE bioproject_id = ?",
                        (row[0],),
                    )
                    deleted += 1
                    current_size_mb = self.db_path.stat().st_size / 1e6
                    if current_size_mb <= target_size_mb:
                        break
                conn.commit()
            new_size_mb = self.db_path.stat().st_size / 1e6
            logger.info(
                f"📦 Publication cache cleanup: removed {deleted} entries, "
                f"{cache_size_mb:.1f} MB → {new_size_mb:.1f} MB"
            )
        except sqlite3.Error as e:
            logger.error(f"Publication cache cleanup failed: {e}")
        return deleted