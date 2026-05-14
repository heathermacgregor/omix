"""
Asynchronous ENA data fetcher.

Always requests ALL available fields from the ENA API and supports batched,
cached retrieval of samples, runs, experiments, studies, and taxonomies.
"""

import asyncio
import time
from hashlib import sha256
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd

from .cache import SQLiteCacheManager
from ..constants import ENA_API_URL


class ENAFetcher:
    """Low‑level async HTTP client for the ENA Portal API.

    Key behaviours:
    - Every API call uses `fields=all` – no column is ever omitted.
    - Results are cached via the provided `SQLiteCacheManager`.
    - Supports concurrent chunked requests with exponential backoff.
    """

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def __init__(
        self,
        email: str,
        max_concurrent: int = 10,
        chunk_size: int = 100,
        progress: Any = None,
        progress_task_id: Optional[int] = None,
        cache_manager: Optional[SQLiteCacheManager] = None,
        log_interval: int = 10,
        fetch_phases: bool = True,   # kept for API compatibility; ignored
    ):
        from omix.logging_utils import get_logger
        self.logger = get_logger("omix.ena.fetcher")
        self.email = email
        self.max_concurrent = max_concurrent
        self.chunk_size = chunk_size
        self.session: Optional[aiohttp.ClientSession] = None
        self.biosamples_cache: Dict[str, Dict] = {}
        self.progress = progress
        self.progress_task_id = progress_task_id
        self.cache_manager = cache_manager
        self.log_interval = log_interval
        self.last_log_time = 0.0
        self.max_retries = 3
        self.initial_backoff = 2

    # ------------------------------------------------------------------
    # Context manager (async with)
    # ------------------------------------------------------------------

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            use_dns_cache=True,
        )
        timeout = aiohttp.ClientTimeout(total=150, connect=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": f"omix ENAFetcher/1.0 ({self.email})"},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_cache_key(self, prefix: str, *args) -> str:
        """Create a stable SHA‑256 key for caching."""
        representation = f"{prefix}:" + ":".join(map(str, args))
        return sha256(representation.encode()).hexdigest()

    def _update_progress(self, advance: int = 1, message: Optional[str] = None):
        if self.progress is not None and self.progress_task_id is not None:
            try:
                self.progress.update(self.progress_task_id, advance=advance)
            except Exception as e:
                if message:
                    self.logger.debug(f"{message} (progress update failed: {e})")

    # ------------------------------------------------------------------
    # Generic batched fetch (used by all specialised methods)
    # ------------------------------------------------------------------

    async def fetch_ena_data_in_batches(
        self,
        result_type: str,
        query_key: str,
        accessions: List[str],
        fields: Optional[str] = None,          # ignored – we always use 'all'
        **kwargs,
    ) -> List[Dict]:
        """Generic batch fetch for any ENA result type (sample, run, experiment, …)."""
        if not accessions:
            return []

        unique_accessions = list(set(accessions))
        chunks = [
            unique_accessions[i:i + self.chunk_size]
            for i in range(0, len(unique_accessions), self.chunk_size)
        ]

        progress_obj = kwargs.get('progress_obj', self.progress)
        with_progress = kwargs.get('with_progress_bar', False)
        task_id = None
        if with_progress and progress_obj:
            task_id = progress_obj.add_task(
                f"[cyan]Fetching {result_type} batches...", total=len(chunks)
            )

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _tracked_fetch_chunk(chunk, chunk_num):
            res = await self._fetch_chunk(
                semaphore, result_type, query_key, chunk, chunk_num, len(chunks)
            )
            if with_progress and progress_obj and task_id is not None:
                progress_obj.advance(task_id, 1)
            return res

        tasks = [_tracked_fetch_chunk(chunk, i + 1) for i, chunk in enumerate(chunks)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        if with_progress and progress_obj and task_id is not None:
            progress_obj.remove_task(task_id)

        all_results: List[Dict] = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Chunk processing failed: {result}")
            elif isinstance(result, list):
                all_results.extend(result)
        return all_results

    async def _fetch_chunk(
        self,
        semaphore: asyncio.Semaphore,
        result_type: str,
        query_key: str,
        chunk: List[str],
        chunk_num: int,
        total_chunks: int,
    ) -> List[Dict]:
        """Fetch a single chunk of accessions from the ENA API (ALL fields)."""
        async with semaphore:
            # Check cache
            key = self._get_cache_key("ena_chunk", result_type, query_key, sorted(chunk))
            if self.cache_manager:
                cached = await self.cache_manager.get(key)
                if cached is not None:
                    return cached

            if not self.session or self.session.closed:
                return []

            query = " OR ".join(f'{query_key}="{acc}"' for acc in chunk)
            params = {
                "result": result_type,
                "query": query,
                "fields": "all",            # <-- ALWAYS all fields
                "format": "json",
                "limit": 0,
            }

            for attempt in range(self.max_retries):
                try:
                    async with self.session.get(ENA_API_URL, params=params) as response:
                        if response.status == 204:
                            return []
                        response.raise_for_status()
                        data = await response.json()
                        if self.cache_manager and data:
                            await self.cache_manager.set(key, data)
                        return data
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.initial_backoff * (2 ** attempt))
            return []

    # ------------------------------------------------------------------
    # Biosamples (sample metadata)
    # ------------------------------------------------------------------

    async def fetch_biosamples_batch(
        self, accessions: List[str], **kwargs
    ) -> Dict[str, Dict]:
        """Fetch biosample metadata for a list of accessions (ALL fields)."""
        if not accessions:
            return {}

        unique = list(set(accessions))
        chunk_size = kwargs.get('chunk_size', 50)
        timeout = kwargs.get('timeout', 60)
        chunks = [unique[i:i + chunk_size] for i in range(0, len(unique), chunk_size)]

        progress_obj = kwargs.get('progress_obj', getattr(self, 'progress', None))
        with_progress = kwargs.get('with_progress_bar', False)
        task_id = None
        if with_progress and progress_obj:
            task_id = progress_obj.add_task(
                "📥 Fetching biosamples (all fields)...", total=len(chunks)
            )

        max_conc = min(getattr(self, 'max_concurrent', 5), 5)
        semaphore = asyncio.Semaphore(max_conc)

        tasks = [
            self._fetch_biosamples_chunk(semaphore, chunk, timeout,
                                         with_progress, progress_obj, task_id)
            for chunk in chunks
        ]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

        if with_progress and progress_obj and task_id:
            progress_obj.remove_task(task_id)

        merged: Dict[str, Dict] = {}
        for res in chunk_results:
            if isinstance(res, dict):
                merged.update(res)
            elif isinstance(res, Exception):
                self.logger.warning(f"Biosample chunk failed: {res}")

        self.biosamples_cache = merged
        return {acc: self.biosamples_cache.get(acc, {})
                for acc in accessions if acc in self.biosamples_cache}

    async def _fetch_biosamples_chunk(
        self,
        semaphore: asyncio.Semaphore,
        chunk: List[str],
        timeout: int,
        with_progress: bool = False,
        progress_obj: Any = None,
        task_id: Any = None,
    ) -> Dict[str, Dict]:
        """Fetch one chunk of biosamples using the ENA search endpoint (ALL fields)."""
        ENA_SEARCH_URL = "https://www.ebi.ac.uk/ena/portal/api/search"

        # Check cache
        key = self._get_cache_key("biosample_chunk", sorted(chunk))
        if self.cache_manager and key:
            cached = await self.cache_manager.get(key)
            if cached is not None:
                return cached

        query_parts = [
            f'(accession="{acc}" OR secondary_sample_accession="{acc}")'
            for acc in chunk
        ]
        query = " OR ".join(query_parts)

        payload = {
            "result": "sample",
            "query": query,
            "fields": "all",             # <-- ALL fields, including custom ones
            "format": "json",
            "limit": 0,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        client_timeout = aiohttp.ClientTimeout(total=timeout)

        async with semaphore:
            for attempt in range(3):
                try:
                    async with self.session.post(
                        ENA_SEARCH_URL, data=payload, headers=headers, timeout=client_timeout
                    ) as resp:
                        if resp.status == 204:
                            return {}
                        if resp.status != 200:
                            text = await resp.text()
                            self.logger.error(f"ENA API error {resp.status}: {text}")
                            return {}
                        data = await resp.json()
                        break
                except (asyncio.TimeoutError, aiohttp.ClientError,
                        aiohttp.ServerDisconnectedError) as e:
                    if attempt == 2:
                        self.logger.error(f"Chunk failed after 3 attempts: {e}")
                        return {}
                    await asyncio.sleep(2 ** (attempt + 1))

        result: Dict[str, Dict] = {}
        for item in data:
            prim_acc = item.get('accession') or item.get('primary_accession')
            sec_acc = item.get('secondary_sample_accession')
            if not prim_acc and not sec_acc:
                continue
            # Keep every field returned by the API (including custom ones)
            flat = {k: v for k, v in item.items() if v is not None and k != 'accession'}
            if prim_acc:
                result[prim_acc] = flat
            if sec_acc:
                result[sec_acc] = flat

        if self.cache_manager and result and key:
            await self.cache_manager.set(key, result)

        if with_progress and progress_obj and task_id:
            progress_obj.advance(task_id, 1)

        if self.progress and self.progress_task_id is not None:
            self.progress.update(self.progress_task_id, advance=len(result))

        return result

    # ------------------------------------------------------------------
    # Specialised batch fetches (runs, experiments, taxonomies)
    # ------------------------------------------------------------------

    async def fetch_runs_batch(self, accessions: List[str]) -> List[Dict]:
        """Fetch run metadata (ALL fields) for the provided sample accessions."""
        return await self.fetch_ena_data_in_batches(
            "read_run", "sample_accession", accessions
        )

    async def fetch_experiments_batch(self, accessions: List[str]) -> List[Dict]:
        """Fetch experiment metadata (ALL fields)."""
        return await self.fetch_ena_data_in_batches(
            "experiment", "experiment_accession", accessions
        )

    async def fetch_taxonomies(self, taxon_ids: List[str], **kwargs) -> List[Dict]:
        """Fetch taxonomy lineages (ALL fields)."""
        if not taxon_ids:
            return []
        return await self.fetch_ena_data_in_batches(
            "taxonomy", "tax_id", taxon_ids
        )

    # ------------------------------------------------------------------
    # Spatial search (nearby samples)
    # ------------------------------------------------------------------

    async def find_nearby_samples(
        self, lat: float, lon: float, radius: float = 10.0
    ) -> pd.DataFrame:
        """Return samples within a geographic radius (km)."""
        try:
            # Use the dedicated metadata function if available
            from omix.metadata.ena.metadata import get_samples_by_location_async
            res = await get_samples_by_location_async(
                lat=lat, lon=lon, radius=radius,
                email=self.email,
                fetcher=self,
                cache_manager=self.cache_manager,
            )
            return res if res is not None else pd.DataFrame()
        except Exception as e:
            import traceback
            self.logger.error(f"Spatial search failed at ({lat}, {lon}): {e}")
            self.logger.error(traceback.format_exc())
            return pd.DataFrame()