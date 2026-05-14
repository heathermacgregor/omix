"""
Metadata fetching and enrichment subpackage.
"""

from .base import AbstractMetadataFetcher, AbstractMetadataEnricher
from .constants import (
    ONTOLOGY_MAP,
    DEFAULT_COORDINATE_SOURCES,
    DEFAULT_COLUMN_MAPPINGS,
)
from .enrichment import MetadataEnricher
from .manager import MetadataManager
from .file_workflow import (
    enrich_metadata_from_path,
    enrich_metadata_from_path_sync,
)