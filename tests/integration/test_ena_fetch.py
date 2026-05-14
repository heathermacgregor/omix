"""Integration test for ENA project metadata retrieval."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from omix.config import Config
from omix.metadata.ena.cache import SQLiteCacheManager
from omix.metadata.ena.metadata import get_samples_by_bioproject_async


ACCESSION = "PRJNA864623"
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config.debug.yaml"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ena_fetch_returns_records():
    if os.getenv("OMIX_RUN_INTEGRATION") != "1":
        pytest.skip("Set OMIX_RUN_INTEGRATION=1 to run ENA integration tests")

    config = Config(CONFIG_PATH)
    cache = SQLiteCacheManager(config.cache_dir)
    email = config.credentials.ena_email or config.credentials.email

    try:
        df = await get_samples_by_bioproject_async(
            ACCESSION,
            email=email,
            cache_manager=cache,
        )
    finally:
        await cache.close()

    assert not df.empty
    assert "sample_accession" in df.columns
    assert "scientific_name" in df.columns