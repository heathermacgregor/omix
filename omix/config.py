"""
Unified configuration for omix, loadable from YAML files and environment variables.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class Credentials:
    """Holds all API credentials, loaded from config or environment."""

    def __init__(self, cred_dict: Optional[Dict[str, Any]] = None):
        cred = cred_dict or {}
        self.email = cred.get("email") or os.getenv("OMIX_EMAIL", "")
        self.ena_email = cred.get("ena_email") or os.getenv("OMIX_ENA_EMAIL", self.email)
        self.ncbi_api_key = cred.get("ncbi_api_key") or os.getenv("OMIX_NCBI_API_KEY")
        self.llm_api_key = cred.get("llm_api_key") or os.getenv("OMIX_LLM_API_KEY")
        self.dimensions_api_key = cred.get("dimensions_api_key") or os.getenv("OMIX_DIMENSIONS_API_KEY")
        self.ieee_api_key = cred.get("ieee_api_key") or os.getenv("OMIX_IEEE_API_KEY")
        self.mendeley_api_key = cred.get("mendeley_api_key") or os.getenv("OMIX_MENDELEY_API_KEY")
        self.springer_api_key = cred.get("springer_api_key") or os.getenv("OMIX_SPRINGER_API_KEY")
        self.lens_api_key = cred.get("lens_api_key") or os.getenv("OMIX_LENS_API_KEY")


class MetadataConfig:
    """Metadata normalization and enrichment settings."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        self.columns_to_drop = cfg.get("columns_to_drop", [])
        self.force_numeric_columns = cfg.get("force_numeric_columns", ["lat", "lon", "depth", "altitude"])
        self.mappings = cfg.get("mappings", {})
        self.sample_id_column = cfg.get("sample_id_column", "#sampleid")
        self.suffixes_to_collapse = cfg.get("suffixes_to_collapse", [])
        self.exclude_host = cfg.get("exclude_host", False)
        self.enable_geocoding = cfg.get("enable_geocoding", True)


class ENAApiConfig:
    """ENA API specific options."""

    def __init__(self, cfg: Dict[str, Any]):
        self.enabled = cfg.get("enabled", True)
        self.max_concurrent = cfg.get("max_concurrent", 5)
        self.batch_size = cfg.get("batch_size", 100)
        self.cache_ttl_days = cfg.get("cache_ttl_days", 30)
        self.fetch_phases = cfg.get("fetch_phases", True)
        self.phase2_async = cfg.get("phase2_async", True)
        self.cache_write = CacheWriteConfig(cfg.get("cache_write", {}))


class CacheWriteConfig:
    """Batched write configuration for SQLite cache."""

    def __init__(self, cfg: Dict[str, Any]):
        self.batch_size = cfg.get("batch_size", 100)
        self.flush_interval_seconds = cfg.get("flush_interval_seconds", 5.0)


class ApisConfig:
    """Toggle which external APIs are enabled."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        self.enabled = cfg.get("enabled", True)
        seq = cfg.get("sequence", {})
        self.ena = ENAApiConfig(seq.get("ena", {}))


class PublicationConfig:
    """Publication search and analysis settings."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        self.max_concurrent_apis = cfg.get("max_concurrent_apis", 5)
        self.rounds = cfg.get("rounds", 3)
        self.max_pdf_pages = cfg.get("max_pdf_pages", 10)
        self.max_file_size = cfg.get("max_file_size", 15_000_000)
        
        # FEATURE 3: Retry/backoff configuration per API source
        retry_cfg = cfg.get("retry", {})
        self.max_retries = retry_cfg.get("max_retries", 5)
        self.base_delay_seconds = retry_cfg.get("base_delay_seconds", 1.0)
        self.max_delay_seconds = retry_cfg.get("max_delay_seconds", 32.0)


class PathsConfig:
    """Project and dependency paths."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        self.project = Path(cfg.get("project", "."))
        self.cache_dir = Path(cfg.get("cache_dir", self.project / ".cache"))
        self.logs_dir = Path(cfg.get("logs_dir", self.project / "logs"))
        self.primer_db = cfg.get("primer_db")  # optional


class OmicsType:
    """Enumeration of supported omics types."""

    _16S = "16S"
    METAGENOMICS = "metagenomics"

    def __init__(self, name: str):
        self.name = name


class Config:
    """Top‑level configuration object, aggregating all sub‑configs."""

    def __init__(self, config_path: Optional[Path] = None, **overrides):
        self._raw: Dict[str, Any] = {}
        if config_path:
            with open(config_path, "r") as f:
                self._raw = yaml.safe_load(f) or {}

        # Move known overrides into the correct sub-dicts BEFORE building sub‑configs
        cred_overrides = {}
        for key in ("email", "ena_email", "llm_api_key", "ncbi_api_key",
                    "dimensions_api_key", "ieee_api_key", "mendeley_api_key",
                    "springer_api_key", "lens_api_key"):
            if key in overrides:
                cred_overrides[key] = overrides.pop(key)

        self._raw.update(overrides)                     # remaining top-level overrides
        credentials_raw = self._raw.setdefault("credentials", {})
        credentials_raw.update(cred_overrides)          # apply credential overrides

        # Build sub‑configs
        self.credentials = Credentials(self._raw.get("credentials"))
        self.paths = PathsConfig(self._raw.get("paths"))
        self.metadata = MetadataConfig(self._raw.get("metadata"))
        self.apis = ApisConfig(self._raw.get("apis"))
        self.publication = PublicationConfig(self._raw.get("publication"))
        self.omics_type = OmicsType(self._raw.get("omics_type", "16S"))
        self.cache_dir = self.paths.cache_dir
        self.logs_dir = self.paths.logs_dir

    def save(self, path: Path) -> None:
        """Write current configuration to a YAML file."""
        with open(path, "w") as f:
            yaml.dump(self._raw, f)


def load_config(config_path: Path) -> Config:
    """Load configuration from a YAML file and return a Config instance."""
    return Config(config_path)