"""File-based metadata enrichment workflow.

Provides a flexible entry point for enriching tabular metadata files
(CSV/TSV/Parquet/JSON) using the existing MetadataManager pipeline.

Key behaviors:
- Accepts an input file path and infers file format from extension.
- Detects latitude/longitude columns and maps them to canonical `lat`/`lon`.
- Detects accession-like columns and uses them for ENA/SRA metadata lookup.
- Runs `MetadataManager` processing.
- Preserves rows that cannot be geolocated by merging enriched output back into
  the original table (configurable).
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, Union

import pandas as pd

from omix.config import Config, load_config
from omix.metadata.manager import MetadataManager
from omix.logging_utils import get_logger

logger = get_logger("omix.metadata.file_workflow")

# Supported file extensions for input/output
SUPPORTED_INPUT_SUFFIXES = {".csv", ".tsv", ".txt", ".parquet", ".pq", ".json", ".jsonl"}

# Patterns to identify accession-like tokens
_ACCESSION_PATTERNS = (
    re.compile(r"^(?:SRR|ERR|DRR)\d+$", re.IGNORECASE),
    re.compile(r"^(?:SRS|ERS|DRS|SAMEA|SAMN|SAMD)\d+$", re.IGNORECASE),
    re.compile(r"^(?:SRP|ERP|DRP|PRJNA|PRJEB|PRJDB)\d+$", re.IGNORECASE),
)

# Known coordinate column pairs
_COORDINATE_COLUMN_PAIRS = (
    ("lat", "lon"),
    ("latitude", "longitude"),
    ("latitudeparsed", "longitudeparsed"),
    ("decimal_latitude", "decimal_longitude"),
    ("decimal_lat", "decimal_lon"),
    ("gps_latitude", "gps_longitude"),
)

# Regex for coordinate column detection
_COORDINATE_LAT_PATTERN = re.compile(r"(?:^|[_\s])(lat|latitude)(?:$|[_\s])", re.IGNORECASE)
_COORDINATE_LON_PATTERN = re.compile(r"(?:^|[_\s])(lon|long|longitude)(?:$|[_\s])", re.IGNORECASE)

# Hints for accession column detection (name-based)
_ACCESSION_COLUMN_HINTS = (
    "run_accession",
    "sample_accession",
    "study_accession",
    "project_accession",
    "ena_accession",
    "sra_accession",
    "accession",
)

_PROFILE_FIRST_COLUMNS = [
    "#sampleid",
    "run_accession",
    "sample_accession",
    "study_accession",
    "experiment_accession",
    "omics_type",
    "amplicon_gene",
    "subfragment",
    "primer_set",
    "primer_names",
    "primer_sequences",
    "variable_regions",
    "sequencing_details",
    "library_strategy",
    "library_source",
    "library_selection",
    "instrument_model",
    "extraction_protocol_and_kits",
    "pcr_conditions_and_kits",
    "publication_count",
    "publication_dois",
    "lat",
    "lon",
    "location",
    "country",
    "region",
    "sample_category",
    "sample_title",
    "sample_description",
    "scientific_name",
]

_AMPLICON_GENE_PATTERNS = (
    ("16S-18S", re.compile(r"(?:16s.*18s|18s.*16s)", re.IGNORECASE)),
    ("18S rRNA", re.compile(r"(?:18s(?:\s*rna)?|eukaryotic ssu)", re.IGNORECASE)),
    ("ITS", re.compile(r"(?:\bits\b|internal transcribed spacer)", re.IGNORECASE)),
    ("COI", re.compile(r"(?:\bcoi\b|cox1|cox-1)", re.IGNORECASE)),
    ("23S rRNA", re.compile(r"(?:23s(?:\s*rna)?)", re.IGNORECASE)),
    ("28S rRNA", re.compile(r"(?:28s(?:\s*rna)?)", re.IGNORECASE)),
    ("16S rRNA", re.compile(r"(?:16s(?:\s*rna)?|\bssu\b)", re.IGNORECASE)),
    ("trnL", re.compile(r"(?:trnl)", re.IGNORECASE)),
    ("gyrB", re.compile(r"(?:gyrb)", re.IGNORECASE)),
    ("rpoB", re.compile(r"(?:rpob)", re.IGNORECASE)),
    ("nifH", re.compile(r"(?:nifh)", re.IGNORECASE)),
    ("rbcL", re.compile(r"(?:rbcl)", re.IGNORECASE)),
)

_AMPLICON_PRIMER_SET_PATTERNS = (
    ("515fbc/806r", re.compile(r"515fbc\s*[,/ ]+\s*806r", re.IGNORECASE)),
    ("1391f/EukBr", re.compile(r"1391f\s*[,/ ]+\s*eukbr", re.IGNORECASE)),
    ("ITS1fbc/ITS2r", re.compile(r"its1fbc\s*[,/ ]+\s*its2r", re.IGNORECASE)),
    ("515f/806r", re.compile(r"515f\s*[,/ ]+\s*806r", re.IGNORECASE)),
    ("341f/806r", re.compile(r"341f\s*[,/ ]+\s*806r", re.IGNORECASE)),
)

_AMPLICON_SUBFRAGMENT_PATTERNS = (
    ("V1-V2", re.compile(r"\bv1\s*[-/]\s*v2\b", re.IGNORECASE)),
    ("V2-V3", re.compile(r"\bv2\s*[-/]\s*v3\b", re.IGNORECASE)),
    ("V3-V4", re.compile(r"\bv3\s*[-/]\s*v4\b", re.IGNORECASE)),
    ("V4-V5", re.compile(r"\bv4\s*[-/]\s*v5\b", re.IGNORECASE)),
    ("V4", re.compile(r"\bv4\b", re.IGNORECASE)),
    ("V5-V7", re.compile(r"\bv5\s*[-/]\s*v7\b", re.IGNORECASE)),
    ("V6-V8", re.compile(r"\bv6\s*[-/]\s*v8\b", re.IGNORECASE)),
    ("ITS1/2", re.compile(r"its1\s*[/\-]\s*2|its1/2", re.IGNORECASE)),
    ("ITS1", re.compile(r"\bits1\b", re.IGNORECASE)),
    ("ITS2", re.compile(r"\bits2\b", re.IGNORECASE)),
)

_AMPLICON_METADATA_COLUMNS = (
    "library_construction_protocol_run",
    "pcr_isolation_protocol_run",
    "pcr_primers_run",
    "protocol_label_run",
    "sequencing_method_run",
    "target_gene_run",
    "experimental_protocol_run",
    "library_gen_protocol_run",
    "sequencing_primer_provider_run",
    "sequencing_primer_catalog_run",
    "sequencing_primer_lot_run",
    "primer_names_run",
    "primer_sequences_run",
    "variable_regions_run",
    "sequencing_details_run",
    "study_title_run",
    "sample_title_run",
    "sample_description_run",
    "description_run",
    "library_construction_protocol",
    "pcr_isolation_protocol",
    "pcr_primers",
    "protocol_label",
    "sequencing_method",
    "target_gene",
    "experimental_protocol",
    "library_gen_protocol",
    "sequencing_primer_provider",
    "sequencing_primer_catalog",
    "sequencing_primer_lot",
    "primer_names",
    "primer_sequences",
    "variable_regions",
    "sequencing_details",
    "study_title",
    "sample_title",
    "sample_description",
    "description",
    "target_gene_biosample",
    "sample_description_biosample",
    "study_title_biosample",
    "protocol_label_biosample",
    "sequencing_method_biosample",
    "library_construction_protocol_biosample",
)

_AMPLICON_RUN_METADATA_COLUMNS = tuple(column for column in _AMPLICON_METADATA_COLUMNS if column.endswith("_run"))
_AMPLICON_SAMPLE_METADATA_COLUMNS = tuple(column for column in _AMPLICON_METADATA_COLUMNS if not column.endswith("_run"))


def _as_text_list(value: Any) -> list[str]:
    """Normalize a value or nested value collection into a flat list of strings."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, dict):
        items = []
        for item in value.values():
            items.extend(_as_text_list(item))
        return items
    if isinstance(value, (list, tuple, set)):
        items = []
        for item in value:
            items.extend(_as_text_list(item))
        return items
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return []
    return [text]


def _join_unique_text(values: Iterable[Any], separator: str = "; ") -> str:
    """Collapse a collection into a stable, de-duplicated display string."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        for text in _as_text_list(value):
            normalized = text.strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(normalized)
    return separator.join(ordered)


def _row_text_blob(row: pd.Series, columns: Iterable[str]) -> str:
    """Build a single lowercase search blob from a row and a set of columns."""
    parts: list[str] = []
    for column in columns:
        if column in row and pd.notna(row[column]):
            parts.extend(_as_text_list(row[column]))
    return " ".join(parts).lower()


def _row_all_text_blob(row: pd.Series) -> str:
    """Build a lowercase search blob from every populated field in a row."""
    return _row_text_blob(row, row.index)


def _normalize_column_name(column_name: Any) -> str:
    """Normalize a column name for case- and punctuation-insensitive matching."""
    return re.sub(r"[^a-z0-9]+", "_", str(column_name).strip().lower()).strip("_")


def _row_text_blob_by_columns(row: pd.Series, columns: Iterable[str]) -> str:
    """Build a lowercase search blob from a row using normalized column matching."""
    lookup = {_normalize_column_name(column): column for column in row.index}
    selected_columns: list[str] = []
    for column in columns:
        actual_column = lookup.get(_normalize_column_name(column))
        if actual_column and actual_column not in selected_columns:
            selected_columns.append(actual_column)
    return _row_text_blob(row, selected_columns)


def _infer_amplicon_gene(text: str) -> Optional[str]:
    """Infer the amplicon gene from a text blob."""
    for label, pattern in _AMPLICON_GENE_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _infer_primer_set(text: str) -> Optional[str]:
    """Infer a primer set name from a text blob."""
    for label, pattern in _AMPLICON_PRIMER_SET_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _infer_subfragment(text: str) -> Optional[str]:
    """Infer the amplified subfragment or variable region from a text blob."""
    for label, pattern in _AMPLICON_SUBFRAGMENT_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _infer_gene_from_primer_set(primer_set: Optional[str]) -> Optional[str]:
    """Map common primer sets to likely amplicon genes."""
    if not primer_set:
        return None
    normalized = str(primer_set).strip().lower()
    if normalized in {"515fbc/806r", "515f/806r", "341f/806r"}:
        return "16S rRNA"
    if normalized == "1391f/eukbr":
        return "18S rRNA"
    if normalized == "its1fbc/its2r":
        return "ITS"
    return None


def _infer_subfragment_from_primer_set(primer_set: Optional[str]) -> Optional[str]:
    """Map common primer sets to a likely subfragment when it is not explicit."""
    if not primer_set:
        return None
    normalized = str(primer_set).strip().lower()
    if normalized in {"515fbc/806r", "515f/806r", "341f/806r"}:
        return "V4"
    if normalized == "1391f/eukbr":
        return "V9"
    if normalized == "its1fbc/its2r":
        return "ITS1"
    return None


def _infer_primer_names_and_sequences(text: str) -> tuple[Optional[str], Optional[str]]:
    """Extract primer labels and sequences from metadata text.

    Handles common patterns such as "FWD:...; REV:..." or "FWD/REV".
    Only matches explicit FWD: or REV: patterns; does not fall back to
    regex pattern matching as that produces too many false positives.
    """
    if not text:
        return None, None

    label_patterns = (
        ("FWD", re.compile(r"\b(?:fwd|forward)\s*[:=]\s*([^;,:\n\r]+)", re.IGNORECASE)),
        ("REV", re.compile(r"\b(?:rev|reverse)\s*[:=]\s*([^;,:\n\r]+)", re.IGNORECASE)),
    )
    primer_names: list[str] = []
    primer_sequences: list[str] = []

    for label, pattern in label_patterns:
        matches = [match.strip().rstrip(".,;:") for match in pattern.findall(text)]
        if matches:
            primer_names.append(label)
            primer_sequences.extend(matches)

    # Only return results if we actually found FWD: or REV: patterns
    if not primer_sequences:
        return None, None

    names_text = _join_unique_text(primer_names) or None
    sequences_text = _join_unique_text(primer_sequences) or None
    return names_text, sequences_text


def _is_missing_value(value: Any) -> bool:
    """Return True for empty or placeholder metadata values."""
    if value is None or pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "unknown", "not applicable", "not provided", "not reported"}


def _infer_omics_type(row: pd.Series) -> Optional[str]:
    """Infer the broad omics type from ENA and methodology fields."""
    # Check library_strategy first (ENA field)
    library_strategy = str(row.get("library_strategy", "")).upper()
    if "AMPLICON" in library_strategy:
        return "amplicon"
    if "WGS" in library_strategy or "WHOLE" in library_strategy:
        return "metagenomics"
    if "RNA" in library_strategy:
        return "transcriptomics"
    
    # Check library_source (ENA field)
    library_source = str(row.get("library_source", "")).upper()
    if "METAGENOMIC" in library_source:
        return "metagenomics"
    if "TRANSCRIPTOMIC" in library_source:
        return "transcriptomics"
    if "GENOMIC" in library_source:
        return "genomics"
    
    # Fall back to text search
    blob = _row_text_blob(
        row,
        (
            "library_strategy",
            "library_source",
            "library_selection",
            "library_construction_protocol",
            "pcr_isolation_protocol",
            "protocol_label",
            "sequencing_method",
            "target_gene",
            "sequencing_details",
            "sample_description",
            "study_title",
            "primer_names",
            "primer_sequences",
            "variable_regions",
        ),
    )
    if any(keyword in blob for keyword in ("amplicon", "metabarcod", "16s", "18s", "its", "coi")):
        return "amplicon"
    if any(keyword in blob for keyword in ("metatranscript", "rna-seq", "rna seq", "transcriptom", "mrna")):
        return "transcriptomics"
    if any(keyword in blob for keyword in ("metagenom", "shotgun", "wgs", "whole genome", "whole metagenome")):
        return "metagenomics"
    if any(keyword in blob for keyword in ("genome", "genomic", "wgs")):
        return "genomics"
    if any(keyword in blob for keyword in ("proteom", "metaproteom")):
        return "proteomics"
    if any(keyword in blob for keyword in ("metabolom", "metabolomics")):
        return "metabolomics"
    return None


def _collect_publication_methodology(results: Dict[str, list[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate publication methodology fields by accession."""
    aggregated: Dict[str, Dict[str, Any]] = {}
    fields = [
        "primer_names",
        "primer_sequences",
        "variable_regions",
        "extraction_protocol_and_kits",
        "pcr_conditions_and_kits",
        "sequencing_details",
    ]
    for accession, records in results.items():
        bucket: Dict[str, list[str]] = {field: [] for field in fields}
        statuses: list[str] = []
        for record in records:
            methodology = record.get("methodology_details") or {}
            statuses.append(str(record.get("status", "")).strip())
            for field in fields:
                bucket[field].extend(_as_text_list(methodology.get(field)))
        summary: Dict[str, Any] = {
            field: _join_unique_text(bucket[field]) or None
            for field in fields
        }
        summary["primer_set"] = summary["primer_names"] or _infer_primer_set(_join_unique_text(bucket["primer_names"]) or "")
        summary["subfragment"] = summary["variable_regions"] or _infer_subfragment(_join_unique_text(bucket["variable_regions"]) or "")
        summary["publication_statuses"] = _join_unique_text(statuses)
        summary["omics_type"] = _infer_omics_type(pd.Series(summary))
        combined_text = " ".join(
            text for text in (
                summary.get("primer_names"),
                summary.get("primer_sequences"),
                summary.get("variable_regions"),
                summary.get("sequencing_details"),
                summary.get("extraction_protocol_and_kits"),
                summary.get("pcr_conditions_and_kits"),
            )
            if text
        ).lower()
        if not summary["omics_type"]:
            summary["omics_type"] = _infer_omics_type(pd.Series({"study_title": combined_text, "sequencing_details": combined_text, "sample_description": combined_text}))
        summary["amplicon_gene"] = _infer_amplicon_gene(combined_text)
        aggregated[accession] = summary
    return aggregated


def _apply_profile_filters(
    df: pd.DataFrame,
    *,
    omics_type: Optional[str] = None,
    amplicon_gene: Optional[str] = None,
    primer_set: Optional[str] = None,
    subfragment: Optional[str] = None,
) -> pd.DataFrame:
    """Filter a dataframe by omics / amplicon profile labels."""
    result = df.copy()

    def _keep(column_names: tuple[str, ...], requested: Optional[str]) -> None:
        nonlocal result
        if not requested or result.empty:
            return
        requested_values = [token.strip().lower() for token in str(requested).split(",") if token.strip()]
        if not requested_values:
            return
        mask = pd.Series(False, index=result.index)
        for column_name in column_names:
            if column_name not in result.columns:
                continue
            series = result[column_name].fillna("").astype(str).str.lower()
            for token in requested_values:
                mask |= series.str.contains(re.escape(token), na=False)
        result = result[mask].copy()

    _keep(("omics_type", "library_strategy", "library_source", "library_selection", "sequencing_details"), omics_type)
    _keep(("amplicon_gene", "primer_names", "primer_sequences", "variable_regions", "sequencing_details"), amplicon_gene)
    _keep(("primer_set", "primer_names", "primer_sequences"), primer_set)
    _keep(("subfragment", "variable_regions", "sequencing_details"), subfragment)
    return result


def _order_profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Move profile, location, and sample detail columns to the front."""
    ordered_columns = [col for col in _PROFILE_FIRST_COLUMNS if col in df.columns]
    ordered_columns.extend([col for col in df.columns if col not in ordered_columns])
    return df.loc[:, ordered_columns]


def _build_composition_report(
    df: pd.DataFrame,
    *,
    filters: Optional[Dict[str, Optional[str]]] = None,
    top_n: int = 10,
) -> Dict[str, Any]:
    """Create a compact composition report for the enriched dataset."""
    report: Dict[str, Any] = {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "filters": {k: v for k, v in (filters or {}).items() if v},
        "composition": {},
    }
    for column_name in (
        "sample_category",
        "omics_type",
        "amplicon_gene",
        "primer_set",
        "subfragment",
        "library_strategy",
        "library_source",
        "library_selection",
        "instrument_model",
        "sequencing_details",
    ):
        if column_name not in df.columns:
            continue
        counts = df[column_name].fillna("Unknown").astype(str).value_counts(dropna=False).head(top_n)
        report["composition"][column_name] = [
            {
                "value": str(value),
                "count": int(count),
                "fraction": round(float(count) / max(len(df), 1), 4),
            }
            for value, count in counts.items()
        ]
    return report


def _normalize_column_lookup(columns: Iterable[str]) -> Dict[str, str]:
    """Builds a lowercase/trimmed to original column name map."""
    return {str(col).strip().lower(): str(col) for col in columns}


def _is_accession_token(token: str) -> bool:
    """Returns True if the token appears to be an ENA/SRA accession."""
    return any(pattern.match(token) for pattern in _ACCESSION_PATTERNS)


def _split_accession_tokens(value: Any) -> Iterable[str]:
    """Splits potentially multi-accession values into normalized tokens."""
    if pd.isna(value):
        return []
    raw_value = str(value).strip()
    if not raw_value:
        return []
    return [
        token.strip()
        for token in re.split(r"[\s,;|]+", raw_value)
        if token and token.strip()
    ]


def _load_table(input_path: Path, **read_kwargs: Any) -> pd.DataFrame:
    """Loads a metadata table from CSV/TSV/Parquet/JSON based on file extension."""
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(input_path, low_memory=False, **read_kwargs)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(input_path, sep="\t", low_memory=False, **read_kwargs)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(input_path, **read_kwargs)
    if suffix == ".json":
        return pd.read_json(input_path, **read_kwargs)
    if suffix == ".jsonl":
        return pd.read_json(input_path, lines=True, **read_kwargs)
    raise ValueError(
        f"Unsupported input format '{suffix}'. Supported formats: {sorted(SUPPORTED_INPUT_SUFFIXES)}"
    )


def _save_table(df: pd.DataFrame, output_path: Path, **write_kwargs: Any) -> None:
    """Writes an enriched metadata table to disk using extension-driven format."""
    suffix = output_path.suffix.lower()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".csv":
        df.to_csv(output_path, index=False, **write_kwargs)
        return
    if suffix in {".tsv", ".txt"}:
        df.to_csv(output_path, sep="\t", index=False, **write_kwargs)
        return
    if suffix in {".parquet", ".pq"}:
        df.to_parquet(output_path, index=False, **write_kwargs)
        return
    if suffix == ".json":
        df.to_json(output_path, orient="records", **write_kwargs)
        return
    if suffix == ".jsonl":
        df.to_json(output_path, orient="records", lines=True, **write_kwargs)
        return
    raise ValueError(
        f"Unsupported output format '{suffix}'. Supported formats: {sorted(SUPPORTED_INPUT_SUFFIXES)}"
    )


def detect_coordinate_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Detects coordinate columns from common naming conventions."""
    if df.empty:
        return None, None
    col_lookup = _normalize_column_lookup(df.columns)
    for lat_name, lon_name in _COORDINATE_COLUMN_PAIRS:
        lat_col = col_lookup.get(lat_name)
        lon_col = col_lookup.get(lon_name)
        if lat_col and lon_col:
            return lat_col, lon_col

    lat_fallback: Optional[str] = None
    lon_fallback: Optional[str] = None
    for col in df.columns:
        lowered = str(col).strip().lower()
        if lat_fallback is None and _COORDINATE_LAT_PATTERN.search(lowered):
            lat_fallback = str(col)
        if lon_fallback is None and _COORDINATE_LON_PATTERN.search(lowered):
            lon_fallback = str(col)
    return lat_fallback, lon_fallback


def detect_accession_column(
    df: pd.DataFrame,
    min_match_ratio: float = 0.30,
    min_match_count: int = 3,
) -> Optional[str]:
    """Finds the most likely accession column using name hints and value patterns."""
    if df.empty:
        return None
    col_lookup = _normalize_column_lookup(df.columns)

    # 1. Name-based fast path
    for hint in _ACCESSION_COLUMN_HINTS:
        hinted_col = col_lookup.get(hint)
        if hinted_col:
            return hinted_col

    # 2. Value-based scan across string/object columns
    best_col: Optional[str] = None
    best_ratio = 0.0
    best_match_count = 0
    candidate_cols = [
        col for col in df.columns
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col])
    ]
    for col in candidate_cols:
        values = df[col].dropna().head(300)
        if values.empty:
            continue
        total_tokens = 0
        matched_tokens = 0
        for value in values:
            tokens = list(_split_accession_tokens(value))
            total_tokens += len(tokens)
            matched_tokens += sum(1 for token in tokens if _is_accession_token(token))
        if total_tokens == 0:
            continue
        ratio = matched_tokens / total_tokens
        if (
            matched_tokens >= min_match_count
            and ratio >= min_match_ratio
            and (ratio > best_ratio or (ratio == best_ratio and matched_tokens > best_match_count))
        ):
            best_col = str(col)
            best_ratio = ratio
            best_match_count = matched_tokens
    return best_col


def _ensure_sample_id_column(df: pd.DataFrame, sample_id_column: str) -> pd.DataFrame:
    """Ensures the configured sample ID column exists in the DataFrame."""
    output = df.copy()
    col_lookup = _normalize_column_lookup(output.columns)
    normalized_target = sample_id_column.strip().lower()
    actual_target = col_lookup.get(normalized_target)
    if actual_target and actual_target != sample_id_column:
        output = output.rename(columns={actual_target: sample_id_column})
        return output
    if sample_id_column in output.columns:
        return output
    fallback_candidates = (
        "#sampleid",
        "sample_id",
        "sample id",
        "sampleid",
        "run_accession",
        "sample_accession",
        "accession",
    )
    for candidate in fallback_candidates:
        actual = col_lookup.get(candidate)
        if actual:
            output[sample_id_column] = output[actual]
            return output
    output[sample_id_column] = output.index.astype(str)
    return output


def _fill_missing_from_source(target_df: pd.DataFrame, source_df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Fills null values in target from source for the requested columns."""
    output = target_df.copy()
    for col in columns:
        if col not in source_df.columns:
            continue
        if col not in output.columns:
            output[col] = source_df[col]
            continue
        mask = output[col].isna()
        if mask.any():
            output.loc[mask, col] = source_df.loc[mask, col]
    return output


def _merge_preserving_rows(base_df: pd.DataFrame, enriched_df: pd.DataFrame, sample_id_column: str) -> pd.DataFrame:
    """Merges enriched rows back into the original table to preserve all input rows."""
    if sample_id_column not in base_df.columns or sample_id_column not in enriched_df.columns:
        return enriched_df.copy()
    merged = base_df.merge(enriched_df, on=sample_id_column, how="left", suffixes=("", "_enriched"))
    for col in enriched_df.columns:
        if col == sample_id_column:
            continue
        enriched_col = f"{col}_enriched"
        if enriched_col not in merged.columns:
            continue
        if col in base_df.columns:
            enriched_values = merged.loc[:, enriched_col]
            base_values = merged.loc[:, col]
            if isinstance(enriched_values, pd.DataFrame):
                enriched_values = enriched_values.iloc[:, 0]
            if isinstance(base_values, pd.DataFrame):
                base_values = base_values.iloc[:, 0]
            merged[col] = enriched_values.combine_first(base_values)
            merged.drop(columns=[enriched_col], inplace=True)
        else:
            merged.rename(columns={enriched_col: col}, inplace=True)
    return merged


def _resolve_config(
    config: Optional[Config],
    config_path: Optional[Union[str, Path]],
) -> Config:
    """Resolves config from explicit object, path, or defaults."""
    if config is not None:
        return config
    if config_path is not None:
        return load_config(Path(config_path))
    return Config()


def _derive_default_output_path(input_path: Path) -> Path:
    """Creates a default enriched output path next to the input file."""
    return input_path.with_name(f"{input_path.stem}_enriched{input_path.suffix}")


async def enrich_metadata_from_path(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    *,
    config: Optional[Config] = None,
    config_path: Optional[Union[str, Path]] = None,
    sample_id_column: Optional[str] = None,
    enable_ena_lookup: bool = True,
    preserve_all_rows: bool = True,
    save_output: bool = True,
    read_kwargs: Optional[Dict[str, Any]] = None,
    write_kwargs: Optional[Dict[str, Any]] = None,
    omics_type: Optional[str] = None,
    amplicon_gene: Optional[str] = None,
    primer_set: Optional[str] = None,
    subfragment: Optional[str] = None,
    report_output: Optional[Union[str, Path]] = None,
) -> pd.DataFrame:
    """Runs the metadata enrichment workflow for a file path.

    Args:
        input_path: Metadata table path (.csv, .tsv, .parquet, .json, .jsonl).
        output_path: Output path; defaults to ``<stem>_enriched<suffix>``.
        config: Pre-loaded `Config` object.
        config_path: Path to config YAML (used when `config` is not provided).
        sample_id_column: Optional override for sample ID column name.
        enable_ena_lookup: If True, run ENA/SRA enrichment when accession-like
            identifiers are detected.
        preserve_all_rows: If True, rows dropped by strict geolocation checks in
            `MetadataManager` are merged back into the final output.
        save_output: If True, write enriched output to disk.
        read_kwargs: Optional kwargs forwarded to pandas readers.
        write_kwargs: Optional kwargs forwarded to pandas writers.
        omics_type: Optional filter for omics type (amplicon, metagenomics, etc.).
        amplicon_gene: Optional filter for amplicon gene (16S, 18S, ITS, etc.).
        primer_set: Optional filter for primer set names.
        subfragment: Optional filter for subfragments (variable regions).
        report_output: Optional path to save a composition report as JSON.

    Returns:
        Enriched metadata DataFrame.
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Input metadata file not found: {input_file}")

    cfg = _resolve_config(config=config, config_path=config_path)
    resolved_sample_id = sample_id_column or cfg.metadata.sample_id_column

    read_kwargs = read_kwargs or {}
    write_kwargs = write_kwargs or {}

    raw_df = _load_table(input_file, **read_kwargs)
    if raw_df.empty:
        raise ValueError("Input metadata table is empty.")

    working_df = _ensure_sample_id_column(raw_df, resolved_sample_id)

    detected_lat_col, detected_lon_col = detect_coordinate_columns(working_df)
    if detected_lat_col and detected_lon_col:
        if "lat" not in working_df.columns:
            working_df["lat"] = pd.to_numeric(working_df[detected_lat_col], errors="coerce")
        if "lon" not in working_df.columns:
            working_df["lon"] = pd.to_numeric(working_df[detected_lon_col], errors="coerce")

    detected_accession_col = detect_accession_column(working_df)
    if detected_accession_col:
        if detected_accession_col != "study_accession" and "run_accession" not in working_df.columns:
            working_df["run_accession"] = working_df[detected_accession_col]
        elif detected_accession_col != "study_accession":
            working_df["run_accession"] = working_df["run_accession"].combine_first(
                working_df[detected_accession_col]
            )

    effective_sample_id = resolved_sample_id

    if enable_ena_lookup and detected_accession_col and cfg.apis.enabled and cfg.apis.ena.enabled:
        try:
            from .ena import ENAEnrichmentPipeline
            async with ENAEnrichmentPipeline(cfg) as pipeline:
                ena_enriched = await pipeline.enrich_samples(working_df)
            # If the ENA enrichment expanded a single study into many samples,
            # prefer replacing the working table with the full project sample
            # table rather than trying to 'fill' a single-row table (which
            # loses samples). Otherwise, fill missing values into the base
            # working table.
            try:
                if isinstance(ena_enriched, (list, dict)):
                    # normalize to DataFrame
                    import pandas as _pd
                    ena_enriched = _pd.DataFrame(ena_enriched)
                if hasattr(ena_enriched, 'shape') and ena_enriched.shape[0] > working_df.shape[0]:
                    # Expanded ENA study lookups should be keyed by a stable,
                    # row-unique accession rather than the original one-row
                    # sample identifier. Prefer run accessions so outputs can
                    # be consumed at the sequencing-run level.
                    if 'run_accession' in ena_enriched.columns:
                        effective_sample_id = 'run_accession'
                    elif 'sample_accession' in ena_enriched.columns:
                        effective_sample_id = 'sample_accession'

                    ena_enriched = _ensure_sample_id_column(ena_enriched, effective_sample_id)
                    if (
                        effective_sample_id != resolved_sample_id
                        and resolved_sample_id in ena_enriched.columns
                        and effective_sample_id in ena_enriched.columns
                    ):
                        ena_enriched[resolved_sample_id] = ena_enriched[effective_sample_id]
                    working_df = ena_enriched
                else:
                    working_df = _fill_missing_from_source(
                        working_df,
                        ena_enriched,
                        columns=(
                            "run_accession",
                            "sample_accession",
                            "lat",
                            "lon",
                            "collection_date",
                            "location_confidence",
                            "collection_date_precision",
                            "sample_title",
                            "scientific_name",
                            "country",
                        ),
                    )
            except Exception:
                # Fallback to conservative fill if anything unexpected occurs
                working_df = _fill_missing_from_source(
                    working_df,
                    ena_enriched,
                    columns=(
                        "run_accession",
                        "sample_accession",
                        "lat",
                        "lon",
                        "collection_date",
                        "location_confidence",
                        "collection_date_precision",
                        "sample_title",
                        "scientific_name",
                        "country",
                    ),
                )
        except Exception as exc:
            logger.warning(
                "ENA lookup phase failed for input %s. Continuing with base metadata. Error: %s",
                input_file,
                exc,
            )
        else:
            # We already performed a full ENA enrichment for the file; disable
            # ENA enrichment for the later manager pipeline to avoid double runs
            # (which can cause merge/index mismatches when project -> sample
            # expansions occur).
            try:
                cfg.apis.ena.enabled = False
            except Exception:
                pass

    manager = MetadataManager(
        metadata=working_df,
        config=cfg,
        sample_id_column=effective_sample_id,
    )
    enriched_df = await manager.run_pipeline()

    # ---- Publication integration: enrich rows with found publications ----
    pub_methodology_by_accession: Dict[str, Dict[str, Any]] = {}
    try:
        # Build publication sources similar to the CLI
        from omix.publications.apis import (
            CrossrefAPI,
            EuropePMCAPI,
            NCBIAPI,
            SemanticScholarAPI,
            ArxivAPI,
            BioarxivAPI,
            CoreAPI,
            DataciteAPI,
            DOAJAPI,
            PLOSAPI,
            UnpaywallAPI,
            ZenodoAPI,
        )
        from omix.publications.extractors.omics import SixteenSExtractor
        from omix.publications.fetcher import PublicationFetcher
        from omix.publications.cache import PublicationCache

        pubs_cache = PublicationCache(cfg.paths.cache_dir / "publications.db")
        sources = [
            CrossrefAPI(cfg.credentials.email),
            EuropePMCAPI(cfg.credentials.email),
            NCBIAPI(cfg.credentials.email, cfg.credentials.ncbi_api_key),
            SemanticScholarAPI(cfg.credentials.email),
            ArxivAPI(cfg.credentials.email),
            BioarxivAPI(cfg.credentials.email),
            CoreAPI(cfg.credentials.email),
            DataciteAPI(cfg.credentials.email),
            DOAJAPI(cfg.credentials.email),
            PLOSAPI(cfg.credentials.email),
            UnpaywallAPI(cfg.credentials.email),
            ZenodoAPI(cfg.credentials.email),
        ]
        extractor = SixteenSExtractor(api_key=cfg.credentials.llm_api_key or "", primer_db=None)
        pub_fetcher = PublicationFetcher(cfg, sources, extractor, pubs_cache)

        study_accs = (
            enriched_df["study_accession"].dropna().unique().tolist()
            if "study_accession" in enriched_df.columns
            else []
        )
        if study_accs:
            # We're inside an async function; call the async method directly.
            results = await pub_fetcher.fetch_and_analyze(study_accs)
            # Aggregate methodology details by accession
            pub_methodology_by_accession = _collect_publication_methodology(results)
            # Map accession -> comma-separated DOIs and counts
            acc_to_dois = {
                acc: [r.get("doi") for r in recs if r.get("doi")]
                for acc, recs in results.items()
            }
            enriched_df["publication_count"] = enriched_df.get("study_accession", "").map(lambda a: len(acc_to_dois.get(a, [])))
            enriched_df["publication_dois"] = enriched_df.get("study_accession", "").map(lambda a: ",".join(acc_to_dois.get(a, [])))
    except Exception:
        logger.debug("Publication integration skipped or failed.")

    # ---- Attach methodology and omics labels from publications ----
    if pub_methodology_by_accession and "study_accession" in enriched_df.columns:
        for study_acc, methodology in pub_methodology_by_accession.items():
            mask = enriched_df["study_accession"] == study_acc
            if not mask.any():
                continue
            for col in ("primer_names", "primer_sequences", "variable_regions", "primer_set", "subfragment", "omics_type", "amplicon_gene", "extraction_protocol_and_kits", "pcr_conditions_and_kits", "sequencing_details"):
                if col not in methodology or not methodology[col]:
                    continue
                if col not in enriched_df.columns:
                    enriched_df[col] = pd.Series([pd.NA] * len(enriched_df), dtype="string")
                else:
                    enriched_df[col] = enriched_df[col].astype("string")
                # Fill only rows where the column is missing
                missing_mask = mask & enriched_df[col].isna()
                if missing_mask.any():
                    enriched_df.loc[missing_mask, col] = methodology[col]

    final_df = (
        _merge_preserving_rows(working_df, enriched_df, effective_sample_id)
        if preserve_all_rows
        else enriched_df
    )

    # ---- Infer omics types and amplicon details from metadata ----
    # Do this AFTER merge to ensure all rows have the correct inferred values.
    for column_name in ("omics_type", "amplicon_gene", "primer_set", "subfragment"):
        if column_name not in final_df.columns:
            final_df[column_name] = None

    def _infer_metadata_row(row: pd.Series) -> pd.Series:
        current_omics = row.get("omics_type")
        current_gene = row.get("amplicon_gene")
        current_primer_set = row.get("primer_set")
        current_subfragment = row.get("subfragment")
        current_primer_names = row.get("primer_names")
        current_primer_sequences = row.get("primer_sequences")
        run_blob = _row_text_blob_by_columns(row, _AMPLICON_RUN_METADATA_COLUMNS)
        sample_blob = _row_text_blob_by_columns(row, _AMPLICON_SAMPLE_METADATA_COLUMNS)
        fallback_blob = _row_all_text_blob(row)
        run_primer_set = _infer_primer_set(run_blob)
        run_gene = _infer_gene_from_primer_set(run_primer_set) or _infer_amplicon_gene(run_blob)
        run_subfragment = _infer_subfragment(run_blob) or _infer_subfragment_from_primer_set(run_primer_set)
        run_primer_names, run_primer_sequences = _infer_primer_names_and_sequences(run_blob)
        sample_primer_names, sample_primer_sequences = _infer_primer_names_and_sequences(sample_blob)
        fallback_primer_names, fallback_primer_sequences = _infer_primer_names_and_sequences(fallback_blob)

        inferred_omics = current_omics if not _is_missing_value(current_omics) else _infer_omics_type(row)
        if _is_missing_value(inferred_omics):
            inferred_omics = current_omics

        if run_primer_set or run_gene or run_subfragment:
            inferred_primer_set = current_primer_set if not _is_missing_value(current_primer_set) else run_primer_set
            inferred_gene = current_gene if not _is_missing_value(current_gene) else run_gene
            inferred_subfragment = current_subfragment if not _is_missing_value(current_subfragment) else run_subfragment
        else:
            inferred_primer_set = current_primer_set if not _is_missing_value(current_primer_set) else (
                _infer_primer_set(sample_blob) or _infer_primer_set(fallback_blob)
            )
            inferred_gene = current_gene if not _is_missing_value(current_gene) else (
                _infer_gene_from_primer_set(inferred_primer_set)
                or _infer_amplicon_gene(sample_blob)
                or _infer_amplicon_gene(fallback_blob)
            )
            inferred_subfragment = current_subfragment if not _is_missing_value(current_subfragment) else (
                _infer_subfragment(sample_blob)
                or _infer_subfragment(fallback_blob)
                or _infer_subfragment_from_primer_set(inferred_primer_set)
            )

        inferred_primer_names = current_primer_names if not _is_missing_value(current_primer_names) else (
            run_primer_names
            or sample_primer_names
            or fallback_primer_names
        )
        inferred_primer_sequences = current_primer_sequences if not _is_missing_value(current_primer_sequences) else (
            run_primer_sequences
            or sample_primer_sequences
            or fallback_primer_sequences
        )

        return pd.Series(
            {
                "omics_type": inferred_omics,
                "amplicon_gene": inferred_gene,
                "primer_set": inferred_primer_set,
                "subfragment": inferred_subfragment,
                "primer_names": inferred_primer_names,
                "primer_sequences": inferred_primer_sequences,
            }
        )

    inferred_metadata = final_df.apply(_infer_metadata_row, axis=1)
    logger.debug(
        "Inferred metadata sample: %s",
        inferred_metadata.head().to_dict(orient="records"),
    )
    for column_name in ("omics_type", "amplicon_gene", "primer_set", "subfragment", "primer_names", "primer_sequences"):
        final_df[column_name] = inferred_metadata[column_name]
    pre_filter_rows = len(final_df)
    final_df = _apply_profile_filters(
        final_df,
        omics_type=omics_type,
        amplicon_gene=amplicon_gene,
        primer_set=primer_set,
        subfragment=subfragment,
    )
    post_filter_rows = len(final_df)
    if post_filter_rows < pre_filter_rows:
        logger.info(
            "Profile filters applied: %d rows → %d rows",
            pre_filter_rows,
            post_filter_rows,
        )

    # ---- Reorder columns with profile fields first ----
    final_df = _order_profile_columns(final_df)

    # ---- Generate composition report ----
    filters_dict = {
        "omics_type": omics_type,
        "amplicon_gene": amplicon_gene,
        "primer_set": primer_set,
        "subfragment": subfragment,
    }
    composition_report = _build_composition_report(final_df, filters=filters_dict)
    logger.info(
        "Composition: %d rows, %d columns. Omics types: %s",
        composition_report["rows"],
        composition_report["columns"],
        {item["value"]: item["count"] for item in composition_report["composition"].get("omics_type", [])},
    )

    if save_output:
        resolved_output = Path(output_path) if output_path else _derive_default_output_path(input_file)
        _save_table(final_df, resolved_output, **write_kwargs)
        logger.info("File enrichment workflow wrote output to %s", resolved_output)

        # Optionally save the composition report
        if report_output:
            import json
            report_path = Path(report_output)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            with open(report_path, "w") as f:
                json.dump(composition_report, f, indent=2)
            logger.info("Composition report saved to %s", report_path)

    logger.info(
        "File enrichment workflow complete: input_rows=%d output_rows=%d filtered=%d detected_coords=(%s,%s) detected_accession_col=%s",
        len(raw_df),
        len(enriched_df),
        len(final_df),
        detected_lat_col,
        detected_lon_col,
        detected_accession_col,
    )
    return final_df


def enrich_metadata_from_path_sync(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Synchronous wrapper for `enrich_metadata_from_path` (async)."""
    return asyncio.run(enrich_metadata_from_path(input_path, output_path=output_path, **kwargs))