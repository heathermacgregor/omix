"""
External API‑based metadata enrichment.

Provides:
- Reverse geocoding (lat/lon → city, country) via Nominatim
- ENVO code → human‑readable label via EBI OLS
- Publication DOI lookup from accession numbers via NCBI E‑utilities
"""

import asyncio
import inspect
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import aiohttp
import numpy as np
import pandas as pd
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.geocoders import Nominatim

from omix.logging_utils import get_logger

logger = get_logger("omix.metadata.enrichment")


class MetadataEnricher:
    """
    Handles external API‑based enrichment of metadata DataFrames.

    Responsibilities:
    1. Reverse‑geocode coordinates (Nominatim) → fills 'location'
    2. Convert ENVO codes to labels (EBI OLS) → replaces codes in ontology columns
    3. Find publication DOIs from accession numbers (NCBI E‑utilities) → fills 'publication_doi'

    Uses SQLite caches for geocoding and ENVO labels to minimise API calls.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        ncbi_api_key: Optional[str] = None,
        cache_path: Optional[Path] = None,
    ):
        self.session = session
        self.ncbi_api_key = ncbi_api_key
        self.cache_path = cache_path

        # NCBI rate‑limiting
        self.ncbi_semaphore = asyncio.Semaphore(10)
        self.ncbi_pacing_lock = asyncio.Lock()
        self.last_ncbi_request_time = 0.0

        # Initialise SQLite caches if a path was given
        if self.cache_path:
            self._initialize_caches()

        # Statistics for reporting
        self.stats = {
            'geocoding': {'total': 0, 'cached': 0, 'failed': 0},
            'envo': {'total': 0, 'cached': 0, 'failed': 0, 'batch_requests': 0},
            'publications': {'total': 0, 'cached': 0, 'failed': 0},
        }

    # ------------------------------------------------------------------ #
    #  Cache initialisation & helpers
    # ------------------------------------------------------------------ #

    def _initialize_caches(self) -> None:
        """Create geocoding and ENVO cache tables if they don't exist."""
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geocoding_cache (
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    location TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (lat, lon)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS envo_cache (
                    code TEXT PRIMARY KEY,
                    label TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geocoding_ts ON geocoding_cache(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_envo_ts ON envo_cache(timestamp)")

    def _get_cached_location(self, lat: float, lon: float) -> Optional[str]:
        """Return cached location string for the given coordinates, if any."""
        if not self.cache_path:
            return None
        try:
            with sqlite3.connect(self.cache_path) as conn:
                row = conn.execute(
                    "SELECT location FROM geocoding_cache WHERE lat = ? AND lon = ?",
                    (lat, lon),
                ).fetchone()
                if row:
                    self.stats['geocoding']['cached'] += 1
                    return row[0]
        except sqlite3.Error as e:
            logger.warning(f"Geocoding cache read error: {e}")
        return None

    def _cache_location(self, lat: float, lon: float, location: str) -> None:
        """Store a location string in the geocoding cache."""
        if not self.cache_path:
            return
        try:
            with sqlite3.connect(self.cache_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO geocoding_cache (lat, lon, location) VALUES (?, ?, ?)",
                    (lat, lon, location),
                )
        except sqlite3.Error as e:
            logger.warning(f"Geocoding cache write error: {e}")

    def _get_cached_envo_codes(self, codes: Set[str]) -> Dict[str, str]:
        """Retrieve ENVO labels from cache for a set of codes."""
        if not self.cache_path or not codes:
            return {}
        try:
            placeholders = ','.join('?' * len(codes))
            with sqlite3.connect(self.cache_path) as conn:
                rows = conn.execute(
                    f"SELECT code, label FROM envo_cache WHERE code IN ({placeholders})",
                    tuple(codes),
                ).fetchall()
                cached = {row[0]: row[1] for row in rows}
                self.stats['envo']['cached'] += len(cached)
                return cached
        except sqlite3.Error as e:
            logger.warning(f"ENVO cache read error: {e}")
            return {}

    def _cache_envo_codes(self, code_label_map: Dict[str, str]) -> None:
        """Store ENVO code→label mappings in the cache."""
        if not self.cache_path or not code_label_map:
            return
        try:
            with sqlite3.connect(self.cache_path) as conn:
                conn.executemany(
                    "INSERT OR REPLACE INTO envo_cache (code, label) VALUES (?, ?)",
                    code_label_map.items(),
                )
        except sqlite3.Error as e:
            logger.warning(f"ENVO cache write error: {e}")

    # ------------------------------------------------------------------ #
    #  Geocoding enrichment
    # ------------------------------------------------------------------ #

    async def enrich_location_from_coords(self, df: pd.DataFrame) -> None:
        """
        Reverse‑geocode valid lat/lon pairs and fill the 'location' column.
        Uses Nominatim with a 1‑request‑per‑second limit.
        """
        if 'location' not in df.columns:
            df['location'] = pd.Series(index=df.index, dtype=object)
        if 'lat' not in df.columns or 'lon' not in df.columns:
            return

        mask = df['location'].isna() & df['lat'].notna() & df['lon'].notna()
        rows = df[mask]
        if rows.empty:
            return

        logger.info(f"Geocoding {len(rows)} rows...")
        semaphore = asyncio.Semaphore(1)  # Nominatim allows 1 req/s

        # Prefer async geopy adapter; fall back to thread executor
        try:
            from geopy.adapters import AioHTTPAdapter
            try:
                adapter = AioHTTPAdapter(session=self.session)
            except TypeError:
                adapter = AioHTTPAdapter()
            geolocator = Nominatim(
                user_agent="omicspub/1.0",
                adapter_factory=lambda: adapter,
            )
        except (ImportError, TypeError):
            logger.warning("AioHTTPAdapter not available; using thread-based geocoding.")
            geolocator = Nominatim(user_agent="omicspub/1.0")

        tasks = [
            self._fetch_single_location(geolocator, semaphore, idx, row['lat'], row['lon'])
            for idx, row in rows.iterrows()
        ]
        results = await asyncio.gather(*tasks)

        for index, location_str in results:
            if location_str:
                df.loc[index, 'location'] = location_str

    async def _fetch_single_location(
        self,
        geolocator,
        semaphore: asyncio.Semaphore,
        index: int,
        lat: float,
        lon: float,
    ) -> Tuple[int, Optional[str]]:
        """Geocode one coordinate pair, with caching and error handling."""
        self.stats['geocoding']['total'] += 1

        # 1. Check cache
        cached = self._get_cached_location(lat, lon)
        if cached:
            return index, cached

        try:
            async with semaphore:
                reverse_fn = getattr(geolocator, 'reverse', None)
                if inspect.iscoroutinefunction(reverse_fn):
                    location = await reverse_fn(f"{lat}, {lon}", exactly_one=True)
                else:
                    loop = asyncio.get_event_loop()
                    location = await loop.run_in_executor(
                        None, partial(reverse_fn, f"{lat}, {lon}", exactly_one=True)
                    )

                if location and hasattr(location, 'raw') and 'address' in location.raw:
                    addr = location.raw['address']
                    city = addr.get('city', addr.get('town', addr.get('village', '')))
                    country = addr.get('country', '')
                    location_str = ", ".join(filter(None, [city, country]))
                    self._cache_location(lat, lon, location_str)
                    return index, location_str
        except (GeocoderTimedOut, GeocoderServiceError, asyncio.TimeoutError) as e:
            logger.warning(f"Geocoding error for ({lat}, {lon}): {e}")
            self.stats['geocoding']['failed'] += 1

        return index, None

    # ------------------------------------------------------------------ #
    #  ENVO code enrichment
    # ------------------------------------------------------------------ #

    async def convert_envo_codes(self, df: pd.DataFrame) -> None:
        """
        Replace ENVO codes (e.g., ENVO:01000023) with human‑readable labels
        in the columns 'env_material', 'env_feature', 'env_biome'.

        Uses the EBI OLS REST API with caching.
        """
        envo_cols = ['env_material', 'env_feature', 'env_biome']
        existing = [c for c in envo_cols if c in df.columns]
        if not existing:
            return

        envo_pattern = re.compile(r'(ENVO[:_]\d+)', re.IGNORECASE)
        all_codes: Set[str] = set()
        for col in existing:
            for val in df[col].dropna().astype(str).unique():
                for match in envo_pattern.findall(val):
                    all_codes.add(match.upper().replace('_', ':'))

        if not all_codes:
            return

        logger.info(f"Resolving {len(all_codes)} ENVO codes...")
        self.stats['envo']['total'] = len(all_codes)

        code_to_label = self._get_cached_envo_codes(all_codes)
        missing = all_codes - set(code_to_label.keys())

        if missing:
            logger.info(f"Fetching {len(missing)} ENVO labels from API...")
            # Batch fetches with controlled concurrency
            semaphore = asyncio.Semaphore(10)
            tasks = [self._fetch_envo_label(code) for code in missing]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in batch_results:
                if isinstance(result, tuple) and result[1]:
                    code_to_label[result[0]] = result[1]
            self.stats['envo']['batch_requests'] += 1
            
            # Cache the newly fetched labels
            new_labels = {c: l for c, l in code_to_label.items() if c in missing}
            self._cache_envo_codes(new_labels)

        # Replace codes in the DataFrame
        for col in existing:
            df[col] = df[col].astype(str).replace(code_to_label, regex=True)

    async def _fetch_envo_label(self, code: str) -> Tuple[str, Optional[str]]:
        """Fetch a single ENVO label from EBI OLS."""
        url = "https://www.ebi.ac.uk/ols/api/ontologies/envo/terms"
        iri = f"http://purl.obolibrary.org/obo/{code.replace(':', '_')}"
        try:
            async with self.session.get(url, params={'iri': iri}, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    terms = data.get('_embedded', {}).get('terms', [])
                    if terms and 'label' in terms[0]:
                        return code, terms[0]['label']
        except Exception as e:
            logger.warning(f"ENVO lookup failed for {code}: {e}")
            self.stats['envo']['failed'] += 1
        return code, None

    # ------------------------------------------------------------------ #
    #  Publication DOI enrichment
    # ------------------------------------------------------------------ #

    async def find_publications(self, df: pd.DataFrame) -> None:
        """
        Find publication DOIs for accession numbers using NCBI E‑utilities.
        Populates the 'publication_doi' column.
        """
        if 'publication_doi' not in df.columns:
            df['publication_doi'] = np.nan

        # Identify accession‑like columns
        acc_pattern = re.compile(
            r'^(run|sample|exp|study|proj|sra|ena|ddbj|bio)_?(acc|alias)$|^acc$',
            re.IGNORECASE,
        )
        acc_cols = [c for c in df.columns if acc_pattern.search(c)]
        if not acc_cols:
            return

        # Prioritise project/study accessions
        priority = ['project', 'study', 'experiment', 'run', 'sample', 'biosample']
        sorted_cols = sorted(
            acc_cols,
            key=lambda c: next((i for i, p in enumerate(priority) if p in c.lower()), len(priority)),
        )
        df['search_accession'] = df[sorted_cols].bfill(axis=1).iloc[:, 0]

        # Only process rows that actually need enrichment
        mask = df['publication_doi'].isna() & df['search_accession'].notna()
        unique_accs = df.loc[mask, 'search_accession'].unique()

        if len(unique_accs) == 0:
            df.drop(columns=['search_accession'], inplace=True)
            return

        logger.info(f"Looking up publications for {len(unique_accs)} accessions...")
        tasks = [self._fetch_single_doi(acc) for acc in unique_accs]
        results = await asyncio.gather(*tasks)

        acc_to_doi = {acc: doi for acc, doi in results if doi}
        if acc_to_doi:
            df['publication_doi'] = df['publication_doi'].fillna(
                df['search_accession'].map(acc_to_doi)
            )

        df.drop(columns=['search_accession'], inplace=True)

    async def _fetch_single_doi(self, accession: str) -> Tuple[str, Optional[str]]:
        """Retrieve a DOI for a single accession using NCBI E‑utilities."""
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        key_param = f"&api_key={self.ncbi_api_key}" if self.ncbi_api_key else ""

        async def get_xml(url: str) -> Optional[ET.Element]:
            for attempt in range(5):
                async with self.ncbi_semaphore:
                    # Pace requests according to NCBI guidelines
                    async with self.ncbi_pacing_lock:
                        now = time.monotonic()
                        wait = (0.11 if self.ncbi_api_key else 0.34) - (now - self.last_ncbi_request_time)
                        if wait > 0:
                            await asyncio.sleep(wait)
                        self.last_ncbi_request_time = time.monotonic()

                    try:
                        async with self.session.get(url, timeout=60) as resp:
                            if resp.status == 429:
                                retry = int(resp.headers.get("Retry-After", 2 * (attempt + 1)))
                                logger.warning(f"NCBI rate limit. Waiting {retry}s...")
                                await asyncio.sleep(retry)
                                continue
                            resp.raise_for_status()
                            return ET.fromstring(await resp.text())
                    except aiohttp.ClientError as e:
                        logger.warning(f"NCBI request error: {e}")
                        await asyncio.sleep(1 * (attempt + 1))
            return None

        pmids: List[str] = []
        # Try bioproject → sra → biosample
        for db in ['bioproject', 'sra', 'biosample']:
            root = await get_xml(f"{base_url}esearch.fcgi?db={db}&term={accession}&retmode=xml{key_param}")
            if root is not None and (uid_elem := root.find('.//Id')) is not None and uid_elem.text:
                uid = uid_elem.text
                link_root = await get_xml(f"{base_url}elink.fcgi?dbfrom={db}&db=pubmed&id={uid}&retmode=xml{key_param}")
                if link_root is not None:
                    pmids = [
                        e.text for e in link_root.findall(".//LinkSetDb[DbTo='pubmed']//Id") if e.text
                    ]
                if pmids:
                    break

        # Fallback: search by accession directly in PubMed
        if not pmids:
            root = await get_xml(f"{base_url}esearch.fcgi?db=pubmed&term={accession}[accn]&retmode=xml{key_param}")
            if root is not None:
                pmids = [e.text for e in root.findall('.//Id') if e.text]

        if pmids:
            summary_root = await get_xml(f"{base_url}esummary.fcgi?db=pubmed&id={pmids[0]}&retmode=xml{key_param}")
            if summary_root is not None:
                doi_elem = summary_root.find(".//Item[@Name='DOI']")
                if doi_elem is not None and doi_elem.text:
                    logger.info(f"Found DOI {doi_elem.text} for {accession}")
                    return accession, doi_elem.text

        return accession, None