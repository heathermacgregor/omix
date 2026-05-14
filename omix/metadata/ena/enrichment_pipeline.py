"""
ENA Enrichment Pipeline – orchestrates retrieval of ALL ENA metadata fields
and merges them into an existing DataFrame.
"""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd

from omix.config import Config
from omix.logging_utils import get_logger
from .cache import SQLiteCacheManager
from .fetcher import ENAFetcher
from .metadata import get_samples_by_bioproject_async

logger = get_logger("omix.ena.enrichment")


class ENAEnrichmentPipeline:
    """
    High‑level orchestrator that attaches complete ENA metadata to a DataFrame.

    Two modes:
    1. If `study_accession` column exists – fetches full project metadata
       (samples, runs, experiments, biosamples, taxonomies) for each study.
    2. Otherwise – fetches runs and biosamples in batch for the sample IDs present.
    """

    def __init__(
        self,
        config: Config,
        cache_manager: Optional[SQLiteCacheManager] = None,
        cache_dir: Optional[Path] = None,
        progress_obj: Any = None,
        use_sra_fallback: bool = False,
    ):
        self.config = config
        self.cache_manager = cache_manager
        if self.cache_manager is None and cache_dir:
            self.cache_manager = SQLiteCacheManager(cache_dir)
        elif self.cache_manager is None:
            default_cache_dir = self.config.paths.cache_dir
            default_cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_manager = SQLiteCacheManager(default_cache_dir)

        self.email = config.credentials.ena_email or config.credentials.email
        if not self.email:
            logger.warning("ENA email not configured; some API calls may be rate-limited.")

        self.session: Optional[aiohttp.ClientSession] = None
        self.owns_session = False
        self.progress_obj = progress_obj
        self.use_sra_fallback = use_sra_fallback

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=10, use_dns_cache=True)
        timeout = aiohttp.ClientTimeout(total=300, connect=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": f"omix ENAEnrichmentPipeline/1.0 ({self.email})"},
        )
        self.owns_session = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.owns_session and self.session:
            await self.session.close()

    async def enrich_samples(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attach all available ENA metadata to the input DataFrame.

        Returns a new DataFrame with additional columns; original columns are preserved.
        """
        if self.session is None:
            await self.__aenter__()

        sample_id_col = self.config.metadata.sample_id_column
        if sample_id_col not in df.columns:
            logger.error(f"Sample ID column '{sample_id_col}' not found in DataFrame.")
            return df.copy()

        result_df = df.copy()

        def _coalesce_column(frame: pd.DataFrame, base_col: str, source_col: str) -> None:
            if base_col in frame.columns and source_col in frame.columns:
                try:
                    frame[base_col] = frame[base_col].combine_first(frame[source_col])
                except Exception:
                    base_series = frame[base_col].astype(object)
                    source_series = frame[source_col].astype(object)
                    frame[base_col] = base_series.where(base_series.notna(), source_series)
                frame.drop(columns=[source_col], inplace=True)
            elif source_col in frame.columns:
                frame.rename(columns={source_col: base_col}, inplace=True)

        # ── Mode 1: study‑centric enrichment ──────────────────────────
        if 'study_accession' in df.columns:
            study_accs = df['study_accession'].dropna().unique().tolist()
            logger.info(f"Fetching full ENA metadata for {len(study_accs)} studies…")
            for study_acc in study_accs:
                try:
                    project_df = await get_samples_by_bioproject_async(
                        study_acc,
                        email=self.email,
                        cache_manager=self.cache_manager,
                    )
                    if project_df.empty and self.use_sra_fallback:
                        # Fallback: try SRA
                        from omix.metadata.ena.sra_fallback import fetch_sra_samples_for_project
                        project_df = await fetch_sra_samples_for_project(
                            study_acc, email=self.email, cache_manager=self.cache_manager
                        )

                    if project_df.empty:
                        continue

                    # Determine merge column dynamically
                    merge_col = None
                    for candidate in ['study_accession', 'run_accession', 'sample_accession']:
                        if candidate in project_df.columns and candidate in result_df.columns:
                            merge_col = candidate
                            break
                    if merge_col is None:
                        # Last resort: align on the sample ID column if it matches
                        if sample_id_col in project_df.columns:
                            merge_col = sample_id_col

                    if merge_col:
                        result_df = result_df.merge(
                            project_df, on=merge_col, how='left', suffixes=('', '_ena')
                        )
                        # Coalesce duplicate columns
                        for col in project_df.columns:
                            if col == merge_col:
                                continue
                            ena_col = col + '_ena'
                            _coalesce_column(result_df, col, ena_col)
                    else:
                        logger.warning(
                            f"No common accession column between project {study_acc} "
                            f"and input DataFrame. Skipping merge."
                        )
                except Exception as e:
                    logger.error(f"Failed to enrich study {study_acc}: {e}")
        # ── Mode 2: sample‑centric enrichment ─────────────────────────
        else:
            sample_ids = df[sample_id_col].dropna().unique().tolist()
            logger.info(f"Fetching ENA metadata for {len(sample_ids)} samples via accession batch…")
            async with ENAFetcher(
                email=self.email,
                max_concurrent=self.config.apis.ena.max_concurrent,
                chunk_size=self.config.apis.ena.batch_size,
                cache_manager=self.cache_manager,
                progress=self.progress_obj,
            ) as fetcher:
                # Fetch runs and biosamples in parallel, plus experiments if possible
                runs_task = fetcher.fetch_runs_batch(sample_ids)
                biosamples_task = fetcher.fetch_biosamples_batch(sample_ids)
                # We don't have experiment accessions directly, but runs contain them
                runs, biosamples = await asyncio.gather(runs_task, biosamples_task)

                # Merge runs
                if runs:
                    runs_df = pd.DataFrame(runs)
                    # Find best merge column
                    merge_col = None
                    for candidate in ['run_accession', 'sample_accession']:
                        if candidate in runs_df.columns and candidate in result_df.columns:
                            merge_col = candidate
                            break
                    if merge_col:
                        result_df = result_df.merge(runs_df, on=merge_col, how='left', suffixes=('', '_ena'))
                        # Coalesce
                        for col in runs_df.columns:
                            if col == merge_col:
                                continue
                            ena_col = col + '_ena'
                            _coalesce_column(result_df, col, ena_col)

                # Merge biosamples
                if biosamples:
                    bio_df = pd.DataFrame.from_dict(biosamples, orient='index')
                    sample_col = 'sample_accession'
                    if sample_col in result_df.columns:
                        result_df = result_df.merge(
                            bio_df, left_on=sample_col, right_index=True,
                            how='left', suffixes=('', '_bio')
                        )
                        for col in bio_df.columns:
                            bio_col = col + '_bio'
                            _coalesce_column(result_df, col, bio_col)

                # Optionally fetch experiments for all runs
                if runs and 'experiment_accession' in runs_df.columns:
                    exp_accs = runs_df['experiment_accession'].dropna().unique().tolist()
                    if exp_accs:
                        logger.debug(f"Fetching experiments for {len(exp_accs)} experiment accessions…")
                        experiments = await fetcher.fetch_experiments_batch(exp_accs)
                        if experiments:
                            exp_df = pd.DataFrame(experiments)
                            exp_merge_col = 'experiment_accession'
                            if exp_merge_col in result_df.columns:
                                result_df = result_df.merge(
                                    exp_df, on=exp_merge_col, how='left', suffixes=('', '_exp')
                                )
                                for col in exp_df.columns:
                                    if col == exp_merge_col:
                                        continue
                                    exp_enrich_col = col + '_exp'
                                    _coalesce_column(result_df, col, exp_enrich_col)

        return result_df