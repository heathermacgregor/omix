"""Core unit tests for omix – configuration, fetcher sessions, metadata manager,
and publication fetcher with mocked APIs."""

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock

import pytest
import yaml

from omix.config import Config, load_config, Credentials, MetadataConfig
from omix.metadata.manager import MetadataManager
from omix.metadata.ena.fetcher import ENAFetcher
from omix.metadata.ena.cache import SQLiteCacheManager
from omix.publications.fetcher import PublicationFetcher
from omix.publications.base import PublicationSource, OmicsExtractor


# --------------------------------------------------------------------------- #
#  Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture
def sample_config_dict():
    """Minimal dict that can be written to a temp YAML file."""
    return {
        "credentials": {
            "email": "test@example.com",
            "ena_email": "ena@example.com",
        },
        "metadata": {
            "sample_id_column": "#sampleid",
            "exclude_host": False,
        },
        "apis": {
            "enabled": True,
            "sequence": {
                "ena": {
                    "enabled": True,
                    "max_concurrent": 2,
                    "batch_size": 10,
                    "cache_ttl_days": 1,
                }
            }
        },
        "paths": {
            "cache_dir": str(Path(tempfile.gettempdir()) / "omix_test_cache"),
        },
    }


@pytest.fixture
def config_file(sample_config_dict, tmp_path):
    """Write config dict to a temporary YAML file and return its path."""
    config_path = tmp_path / "test_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config_dict, f)
    return config_path


@pytest.fixture
def sample_dataframe():
    """Create a minimal DataFrame resembling ENA metadata."""
    import pandas as pd
    return pd.DataFrame({
        "#sampleid": ["sample1", "sample2"],
        "run_accession": ["SRR111", "SRR222"],
        "sample_accession": ["SAMN111", "SAMN222"],
        "lat": [34.05, -118.24],
        "lon": [-118.24, 34.05],
        "collection_date": ["2020-01-01", "2020-02-01"],
    })


# --------------------------------------------------------------------------- #
#  Config tests
# --------------------------------------------------------------------------- #

def test_config_defaults():
    """Ensure defaults are applied when nothing is provided."""
    config = Config()
    assert config.credentials.email == ""
    assert config.metadata.sample_id_column == "#sampleid"
    assert config.apis.ena.enabled is True


def test_load_config_from_file(config_file):
    """Load a config from a YAML file."""
    config = load_config(config_file)
    assert config.credentials.email == "test@example.com"
    assert config.metadata.exclude_host is False
    assert config.apis.ena.max_concurrent == 2


def test_config_overrides(config_file):
    """Keyword arguments override YAML values."""
    config = Config(config_file, email="override@example.com")
    assert config.credentials.email == "override@example.com"


def test_environment_variables(monkeypatch):
    """Credentials should fall back to environment variables."""
    monkeypatch.setenv("OMIX_EMAIL", "env@example.com")
    monkeypatch.setenv("OMIX_ENA_EMAIL", "ena-env@example.com")
    config = Config()
    assert config.credentials.email == "env@example.com"
    assert config.credentials.ena_email == "ena-env@example.com"


# --------------------------------------------------------------------------- #
#  MetadataManager tests
# --------------------------------------------------------------------------- #

def test_manager_init(sample_dataframe, config_file):
    """MetadataManager should accept a DataFrame and Config."""
    config = load_config(config_file)
    manager = MetadataManager(sample_dataframe, config)
    assert manager.sample_id_column == "#sampleid"
    assert manager.df.shape == (2, 6)


def test_manager_empty_df_raises(config_file):
    """MetadataManager should raise ValueError on empty DataFrame."""
    config = load_config(config_file)
    import pandas as pd
    with pytest.raises(ValueError, match="empty"):
        MetadataManager(pd.DataFrame(), config)


# --------------------------------------------------------------------------- #
#  ENAFetcher tests
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_enafetcher_context_manager(config_file):
    """ENAFetcher should create and close an aiohttp session."""
    config = load_config(config_file)
    async with ENAFetcher(email=config.credentials.email) as fetcher:
        assert fetcher.session is not None
        assert not fetcher.session.closed
    assert fetcher.session.closed


@pytest.mark.asyncio
async def test_enafetcher_empty_accessions(config_file):
    """fetch_ena_data_in_batches should return empty list for empty input."""
    config = load_config(config_file)
    async with ENAFetcher(email=config.credentials.email) as fetcher:
        result = await fetcher.fetch_ena_data_in_batches(
            "sample", "accession", []
        )
        assert result == []


# --------------------------------------------------------------------------- #
#  PublicationFetcher tests
# --------------------------------------------------------------------------- #

class MockPublicationSource(PublicationSource):
    """A fake publication source that returns pre‑defined results."""
    def __init__(self, name="mock", results=None):
        self._source_name = name
        self._results = results or []

    @property
    def source_name(self) -> str:
        return self._source_name

    async def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        return self._results[:limit]


class MockOmicsExtractor(OmicsExtractor):
    """A fake omics extractor for testing the pipeline."""
    def get_llm_prompt(self, text_chunk: str) -> Dict[str, str]:
        return {"system": "", "user": text_chunk}

    def get_expected_keys(self) -> List[str]:
        return ["mock_key"]

    def post_process(self, llm_output: Dict[str, Any], source_text: str) -> Dict[str, Any]:
        return {"mock_key": "mock_value", "verification_status": "Unverified"}

    def validate(self, extracted: Dict[str, Any]) -> Dict[str, Any]:
        extracted["verification_status"] = "Verified (mock)"
        return extracted


@pytest.mark.asyncio
async def test_publication_fetcher_basic(config_file, tmp_path):
    """PublicationFetcher should run through the search/analysis cycle."""
    config = load_config(config_file)
    config.paths.cache_dir = tmp_path

    mock_source = MockPublicationSource(results=[
        {
            "doi": "10.1234/test",
            "publication_title": "Test Article",
            "pub_year": "2020",
            "status": "Mock",
        }
    ])

    extractor = MockOmicsExtractor()
    fetcher = PublicationFetcher(config, [mock_source], extractor)

    # Patch the full-text retrieval to avoid real HTTP calls
    with patch.object(fetcher, "_analyze_publication",
                      return_value=({"doi": "10.1234/test",
                                     "methodology_details": {"mock_key": "mock_value"},
                                     "status": "✓ Extraction complete."}, [])):
        results = await fetcher.fetch_and_analyze(["PRJNA123"])
        assert "PRJNA123" in results
        assert len(results["PRJNA123"]) == 1
        assert results["PRJNA123"][0]["status"] == "✓ Extraction complete."


# --------------------------------------------------------------------------- #
#  Cache tests
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_cache_set_get(tmp_path):
    """SQLiteCacheManager should store and retrieve values."""
    cache = SQLiteCacheManager(tmp_path)
    await cache.set("key1", {"hello": "world"})
    value = await cache.get("key1")
    assert value == {"hello": "world"}
    await cache.close()


@pytest.mark.asyncio
async def test_cache_ttl(tmp_path):
    """Expired entries should not be returned."""
    cache = SQLiteCacheManager(tmp_path, ttl_seconds=0)  # immediate expiry
    await cache.set("key2", "value")
    value = await cache.get("key2")
    assert value is None
    await cache.close()


@pytest.mark.asyncio
async def test_cache_bulk(tmp_path):
    """Bulk get should retrieve multiple keys."""
    cache = SQLiteCacheManager(tmp_path)
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)
    bulk = await cache.get_bulk(["a", "b", "missing"])
    assert bulk == {"a": 1, "b": 2}
    await cache.close()