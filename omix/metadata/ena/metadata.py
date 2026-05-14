"""
ENA metadata retrieval – high‑level functions for fetching complete project,
location‑based, and count data.

All functions use the ENAFetcher (always `fields=all`) and the batched‑write cache.
"""

import asyncio
from typing import Dict, List, Optional, Union

import pandas as pd

from .fetcher import ENAFetcher
from .cache import SQLiteCacheManager
from ...logging_utils import get_logger

logger = get_logger("omix.ena.metadata")

# Cap to prevent massive API sweeps from hanging
MAX_SAMPLES_PER_SWEEP = 1000


async def find_nearby_samples_async(
    session,
    latitude: float,
    longitude: float,
    radius: Union[int, float],
    progress=None,
    task_id=None,
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame of samples within a geographic radius (km).

    Uses a geo_circ query against the ENA Portal API.
    """
    # Build query
    query = f"geo_circ({latitude},{longitude},{radius})"
    params = {
        "result": "sample",
        "query": query,
        "fields": "all",               # <-- all fields
        "format": "json",
        "limit": 0,
    }
    try:
        async with session.get(
            "https://www.ebi.ac.uk/ena/portal/api/search", params=params
        ) as response:
            if response.status == 204:
                return pd.DataFrame()
            response.raise_for_status()
            data = await response.json()
            return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        logger.error(f"Nearby sample search failed: {e}")
        return pd.DataFrame()


async def get_samples_by_bioproject_async(
    bioproject_accession: str,
    email: str = "user@example.com",
    max_concurrent: int = 15,
    cache_manager: Optional[SQLiteCacheManager] = None,
    fetcher: Optional[ENAFetcher] = None,
) -> pd.DataFrame:
    """
    Fetch **all** run, sample, biosample, and taxonomy data for a BioProject.

    Returns a DataFrame with every ENA column available.
    """

    async def _get_data(fetcher_instance: ENAFetcher) -> pd.DataFrame:
        # 1. Fetch runs for the project. ENA's sample search for a study is
        # capped, while the run search returns the full project membership.
        samples = await fetcher_instance.fetch_ena_data_in_batches(
            "read_run",
            "study_accession",
            [bioproject_accession],
            fields=None,               # always all fields
            with_progress_bar=False,
        )
        if not samples:
            return pd.DataFrame()

        # Normalize to list of dicts
        if isinstance(samples, list):
            sample_dicts = samples
        elif isinstance(samples, dict):
            # Try common wrapper keys
            for key in ("result", "rows", "samples", "data"):
                if key in samples and isinstance(samples[key], list):
                    sample_dicts = samples[key]
                    break
            else:
                sample_dicts = []
        else:
            sample_dicts = []

        # 2. Build a DataFrame directly from the sample records so we never
        # lose rows when run-level lookups are unavailable.
        df = pd.DataFrame(sample_dicts)
        if df.empty:
            return df

        if "sample_accession" not in df.columns and "accession" in df.columns:
            df["sample_accession"] = df["accession"]
        if "study_accession" not in df.columns:
            df["study_accession"] = bioproject_accession

        # 3. Enrich with runs, biosamples, and taxonomies when available.
        sample_accessions = [
            str(acc)
            for acc in df.get("sample_accession", pd.Series(dtype=object)).dropna().astype(str).tolist()
            if acc
        ]
        if sample_accessions:
            runs_task = fetcher_instance.fetch_runs_batch(sample_accessions)
            biosamples_task = fetcher_instance.fetch_biosamples_batch(sample_accessions)

            tax_ids = list({
                str(s.get("tax_id"))
                for s in sample_dicts
                if isinstance(s, dict) and s.get("tax_id")
            })
            taxonomy_task = fetcher_instance.fetch_taxonomies(tax_ids)

            runs, biosamples, taxonomies = await asyncio.gather(
                runs_task, biosamples_task, taxonomy_task
            )

            if runs:
                runs_df = pd.DataFrame(runs)

                # Keep one row per sample while still surfacing the run/experiment
                # identifiers that belong to that sample.
                if "sample_accession" in runs_df.columns:
                    run_agg: Dict[str, Any] = {}
                    for col in runs_df.columns:
                        if col == "sample_accession":
                            continue
                        if col in {"run_accession", "experiment_accession"}:
                            run_agg[col] = lambda s: ",".join(
                                sorted({str(v) for v in s.dropna().astype(str) if str(v)})
                            )
                        else:
                            run_agg[col] = "first"

                    runs_grouped = runs_df.groupby("sample_accession", dropna=False).agg(run_agg).reset_index()
                    runs_grouped.rename(
                        columns={
                            "run_accession": "run_accessions",
                            "experiment_accession": "experiment_accessions",
                        },
                        inplace=True,
                    )

                    if "run_accessions" in runs_grouped.columns:
                        runs_grouped["run_count"] = runs_grouped["run_accessions"].fillna("").apply(
                            lambda value: 0 if not value else len([item for item in str(value).split(",") if item])
                        )
                    if "experiment_accessions" in runs_grouped.columns:
                        runs_grouped["experiment_count"] = runs_grouped["experiment_accessions"].fillna("").apply(
                            lambda value: 0 if not value else len([item for item in str(value).split(",") if item])
                        )

                    df = df.merge(runs_grouped, on="sample_accession", how="left", suffixes=("", "_run"))

                    # If a single representative run_accession column is present,
                    # keep the first run while still exposing the aggregated list.
                    if "run_accessions" in df.columns and "run_accession" not in df.columns:
                        df["run_accession"] = df["run_accessions"].fillna("").apply(
                            lambda value: str(value).split(",")[0] if str(value).strip() else None
                        )

                # Fetch and attach experiment-level metadata separately so it is
                # available even when multiple runs point to one sample.
                if "experiment_accession" in runs_df.columns:
                    exp_accs = [
                        str(acc)
                        for acc in runs_df["experiment_accession"].dropna().astype(str).tolist()
                        if acc
                    ]
                    if exp_accs:
                        experiments = await fetcher_instance.fetch_experiments_batch(exp_accs)
                        if experiments:
                            exp_df = pd.DataFrame(experiments)
                            if not exp_df.empty and "experiment_accession" in exp_df.columns:
                                exp_df = exp_df.drop_duplicates(subset=["experiment_accession"]).copy()
                                # Merge experiment metadata onto the grouped runs table,
                                # then keep experiment_* columns on the sample table.
                                if "experiment_accessions" in df.columns:
                                    exp_map = exp_df.set_index("experiment_accession")
                                    # For samples with multiple experiments, keep the first
                                    # row from the experiment list for the standard columns
                                    # and preserve the full list in experiment_accessions.
                                    first_exp = (
                                        df["experiment_accessions"]
                                        .fillna("")
                                        .astype(str)
                                        .apply(lambda value: value.split(",")[0] if value else None)
                                    )
                                    df["experiment_accession"] = first_exp
                                    df = df.merge(
                                        exp_df,
                                        on="experiment_accession",
                                        how="left",
                                        suffixes=("", "_experiment"),
                                    )

            if biosamples:
                bio_df = pd.DataFrame.from_dict(biosamples, orient="index")
                if "sample_accession" in df.columns:
                    df = df.merge(
                        bio_df,
                        left_on="sample_accession",
                        right_index=True,
                        how="left",
                        suffixes=("", "_biosample"),
                    )

            if taxonomies and "tax_id" in df.columns:
                tax_df = pd.DataFrame(taxonomies)
                if "tax_id" in tax_df.columns:
                    df = df.merge(tax_df, on="tax_id", how="left", suffixes=("", "_taxonomy"))

        return df

    if fetcher:
        return await _get_data(fetcher)
    else:
        async with ENAFetcher(
            email, max_concurrent, cache_manager=cache_manager
        ) as new_fetcher:
            return await _get_data(new_fetcher)


async def get_samples_by_location_async(
    lat: float,
    lon: float,
    radius: Union[int, float],
    email: str = "user@example.com",
    max_concurrent: int = 50,
    cache_manager: Optional[SQLiteCacheManager] = None,
    fetcher: Optional[ENAFetcher] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame of samples within `radius` km of the given point,
    enriched with their runs and biosamples.
    """

    async def _get_data(fetcher_instance: ENAFetcher) -> pd.DataFrame:
        samples = await fetcher_instance.find_nearby_samples(lat, lon, radius)
        if samples is None or (isinstance(samples, pd.DataFrame) and samples.empty):
            return pd.DataFrame()

        # Extract accessions
        sample_accs = samples["accession"].tolist() if "accession" in samples.columns else []
        if not sample_accs:
            return pd.DataFrame()

        runs = await fetcher_instance.fetch_runs_batch(sample_accs)
        biosamples = await fetcher_instance.fetch_biosamples_batch(sample_accs)

        df = pd.DataFrame(runs) if runs else pd.DataFrame()
        if not df.empty and biosamples:
            bio_df = pd.DataFrame.from_dict(biosamples, orient="index")
            if "sample_accession" in df.columns:
                df = df.merge(bio_df, left_on="sample_accession", right_index=True, how="left")

        if not df.empty:
            df["query_lat"] = lat
            df["query_lon"] = lon

        return df

    if fetcher:
        return await _get_data(fetcher)
    else:
        async with ENAFetcher(
            email, max_concurrent, cache_manager=cache_manager
        ) as new_fetcher:
            return await _get_data(new_fetcher)


async def get_counts_bulk_async(
    accessions: List[str],
    email: str,
    max_concurrent: int = 15,
    chunk_size: int = 50,
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> Dict[str, int]:
    """
    Return the number of samples for each study accession.
    """
    async with ENAFetcher(
        email, max_concurrent, cache_manager=cache_manager
    ) as fetcher:
        all_samples = await fetcher.fetch_ena_data_in_batches(
            "sample",
            "study_accession",
            accessions,
            fields="accession",   # only need accession for counting
            with_progress_bar=False,
        )
        if not all_samples:
            return {acc: 0 for acc in accessions}

        df = pd.DataFrame(all_samples)
        if "study_accession" not in df.columns:
            return {acc: 0 for acc in accessions}

        counts = df["study_accession"].value_counts().to_dict()
        return {acc: counts.get(acc, 0) for acc in accessions}


async def get_n_samples_by_bioproject_async(
    bioproject_accession: str,
    email: str = "user@example.com",
    max_concurrent: int = 15,
    cache_manager: Optional[SQLiteCacheManager] = None,
    fetcher: Optional[ENAFetcher] = None,
) -> int:
    """
    Return the number of samples in a BioProject.
    """
    counts = await get_counts_bulk_async(
        [bioproject_accession],
        email,
        max_concurrent,
        cache_manager=cache_manager,
    )
    return counts.get(bioproject_accession, 0)