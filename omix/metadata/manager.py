"""
MetadataManager – central orchestrator for cleaning, processing, and enriching
omics metadata tables.

Stages:
1. Cleaning: column dropping, deduplication, numeric coercion, unit standardization, etc.
2. Processing: geolocation extraction, ontology inference, date standardization.
3. Enrichment (async): geocoding, ENVO labels, publication DOIs, ENA metadata (full fields).
4. Host categorisation & optional filtering.

After the pipeline, the DataFrame contains normalized, enriched columns ready
for downstream analysis.
"""

import asyncio
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import aiohttp
import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from omix.config import Config
from omix.logging_utils import get_logger
from .constants import (
    DEFAULT_COLUMN_MAPPINGS,
    DEFAULT_CONVERSIONS,
    DEFAULT_COORDINATE_SOURCES,
    DEFAULT_MEASUREMENT_STANDARDS,
    DEFAULT_UNIT_PATTERNS,
    ONTOLOGY_MAP,
    PH_PATTERN,
)
from .enrichment import MetadataEnricher

try:
    from .ena import ENAEnrichmentPipeline
    HAS_ENA_PIPELINE = True
except ImportError:
    HAS_ENA_PIPELINE = False


class MetadataManager:
    """
    Unified class for metadata cleaning, processing, and enrichment.

    Primary entry point: `run_pipeline()` which executes all stages in order.
    """

    # Class-level patterns used across methods
    NUM_PATTERN = re.compile(r'[-+]?\d*\.\d+|[-+]?\d+')
    PH_PATTERN = PH_PATTERN
    DEFAULT_COORDINATE_SOURCES = DEFAULT_COORDINATE_SOURCES
    DEFAULT_COLUMN_MAPPINGS = DEFAULT_COLUMN_MAPPINGS
    DEFAULT_UNIT_PATTERNS = DEFAULT_UNIT_PATTERNS
    DEFAULT_CONVERSIONS = DEFAULT_CONVERSIONS
    DEFAULT_MEASUREMENT_STANDARDS = DEFAULT_MEASUREMENT_STANDARDS
    ONTOLOGY_MAP = ONTOLOGY_MAP

    # Columns that are always preserved in harmonization
    CORE_COLUMNS = ['run_accession', 'sample_accession', 'lat', 'lon', 'collection_date']

    # Mapping of standard ontology keys to aliases for fuzzy harmonization
    TARGET_SCHEMA = {
        "ph": ["ph", "ph_level", "soil_ph", "water_ph", "ph_sensor"],
        "temperature": ["temp", "temperature_c", "temp_c", "water_temp", "soil_temp", "air_temp"],
        "env_type": ["environment", "sample_type", "water_body", "biome"],
        "salinity": ["sal", "salinity_ppt", "salinity_psu", "salt_concentration", "conductivity"],
        "depth": ["depth", "depth_m", "sampling_depth", "water_depth", "altitude_m"],
        "oxygen": ["do", "dissolved_oxygen", "o2_concentration", "oxygen_saturation"],
        "host_age": ["age", "host_age", "subject_age", "age_years"],
        "host_sex": ["sex", "gender", "host_sex"],
    }

    def __init__(
        self,
        metadata: pd.DataFrame,
        config: Config,
        sample_id_column: Optional[str] = None,
    ):
        if metadata.empty:
            raise ValueError("Cannot process an empty metadata DataFrame.")

        self.logger = get_logger("omix.metadata.manager")
        self.config = config
        self.sample_id_column = sample_id_column or config.metadata.sample_id_column
        self.ncbi_api_key = config.credentials.ncbi_api_key
        self.df = metadata.copy()
        self.initial_shape = self.df.shape
        self.original_df_for_enrichment: Optional[pd.DataFrame] = None

        # Report accumulates actions taken during the pipeline
        self.report: Dict[str, Any] = {
            'initial_shape': self.initial_shape,
            'actions': [],
            'columns_dropped': {'unwanted': [], 'duplicate': [], 'merged': []},
            'numeric_coercions': {},
            'categorical_standardizations': {},
            'unit_standardizations': {},
        }

        self.logger.info(f"MetadataManager initialized with shape {self.df.shape}")

    # ========================================================================
    # Public API
    # ========================================================================

    def harmonize(self, similarity_threshold: int = 85) -> pd.DataFrame:
        """Collapse raw columns into a standard schema using fuzzy matching."""
        self.logger.info(f"🧬 Harmonizing {len(self.df.columns)} raw columns...")
        core_cols = self.CORE_COLUMNS
        harmonized_df = self.df[self.df.columns.intersection(core_cols)].copy()
        matched_raw_cols: List[str] = []

        for standard_key, aliases in self.TARGET_SCHEMA.items():
            found_data = pd.Series(index=self.df.index, dtype=object)
            for raw_col in self.df.columns:
                raw_col_lower = raw_col.lower().strip()
                is_alias = raw_col_lower in aliases
                fuzzy_score = fuzz.ratio(raw_col_lower, standard_key)
                if is_alias or fuzzy_score >= similarity_threshold:
                    found_data = found_data.combine_first(self.df[raw_col])
                    matched_raw_cols.append(raw_col)
            if not found_data.isna().all():
                harmonized_df[standard_key] = found_data

        # Keep remaining columns with sufficient coverage
        for col in self.df.columns:
            if col not in matched_raw_cols and col not in harmonized_df.columns:
                if self.df[col].notna().mean() > 0.01:
                    harmonized_df[col] = self.df[col]

        self.logger.info(f"✅ Harmonized shape: {harmonized_df.shape}")
        return harmonized_df

    async def run_pipeline(self) -> pd.DataFrame:
        """Execute the full cleaning → processing → enrichment pipeline."""
        self.logger.info("[!] Starting metadata processing pipeline...")
        self.df = self.df.reindex(sorted(self.df.columns), axis=1)

        # Stage 1 & 2: synchronous cleaning and processing
        self._run_cleaning_steps()
        self._run_processing_steps()

        if self.df.empty:
            self.logger.warning("DataFrame empty after processing; returning as-is.")
            return self.df

        # Stage 3: asynchronous enrichment
        await self._run_enrichment_steps()

        # Stage 4: host categorisation & optional filtering
        self._categorize_and_filter_host()

        self.logger.info(f"[X] Pipeline complete. Final shape: {self.df.shape}")
        return self.df.copy()

    # ========================================================================
    # Stage 1: Cleaning
    # ========================================================================

    def _run_cleaning_steps(self) -> None:
        self.logger.info("--- Stage 1: Cleaning and Standardization ---")
        steps = [
            ("Dropping unwanted columns", self._drop_unwanted_columns),
            ("Removing duplicate columns", self._clean_duplicate_columns),
            ("Cleaning sample IDs and duplicate rows", self._clean_sample_ids),
            ("Coercing columns to numeric", self._clean_numeric_columns),
            ("Standardizing categorical values", self._standardize_categorical_values),
            ("Applying custom filters", self._apply_custom_filters),
            ("Standardizing units", self._standardize_units),
            ("Collapsing suffix columns", self._collapse_all_suffixes),
            ("Consolidating pH columns", self._collapse_ph_columns),
            ("Standardizing column names", self._standardize_column_names),
        ]
        self._execute_steps(steps)

    def _drop_unwanted_columns(self) -> None:
        cols = [c for c in (self.config.metadata.columns_to_drop or []) if c in self.df.columns]
        if cols:
            self.df.drop(columns=cols, inplace=True)
            self.report['columns_dropped']['unwanted'] = cols
            self.logger.info(f"Dropped {len(cols)} unwanted columns.")

    def _clean_duplicate_columns(self) -> None:
        dup = self.df.columns[self.df.columns.duplicated()].unique().tolist()
        if dup:
            self.df = self.df.loc[:, ~self.df.columns.duplicated()]
            self.report['columns_dropped']['duplicate'] = dup
            self.logger.warning(f"Removed duplicate columns: {dup}")

    def _clean_sample_ids(self) -> None:
        if self.sample_id_column not in self.df.columns:
            alts = ['#sampleid', 'sample_id', 'sample id', 'sample name', 'run_accession']
            found = next((a for a in alts if a in self.df.columns), None)
            if found:
                self.logger.warning(f"Creating '{self.sample_id_column}' from '{found}'.")
                self.df[self.sample_id_column] = self.df[found]
            else:
                raise KeyError(f"Sample ID column '{self.sample_id_column}' not found.")

        original = len(self.df)
        self.df[self.sample_id_column] = self.df[self.sample_id_column].astype(str).str.lower().str.strip()
        self.df.dropna(subset=[self.sample_id_column], inplace=True)
        self.df = self.df[self.df[self.sample_id_column] != '']
        self.df.drop_duplicates(subset=[self.sample_id_column], keep='first', inplace=True)
        removed = original - len(self.df)
        if removed:
            self.report['duplicate_rows_removed'] = removed
            self.logger.warning(f"Removed {removed} rows with duplicate/missing sample IDs.")

    def _clean_numeric_columns(self) -> None:
        for col in (self.config.metadata.force_numeric_columns or []):
            if col in self.df.columns and self.df[col].dtype == object:
                before = self.df[col].isna().sum()
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                coerced = self.df[col].isna().sum() - before
                if coerced:
                    self.report['numeric_coercions'][col] = coerced
                    self.logger.debug(f"Coerced {coerced} values to NaN in '{col}'.")

    def _standardize_categorical_values(self) -> None:
        for col, mapping in (self.config.metadata.mappings or {}).items():
            if col in self.df.columns:
                s = self.df[col].astype(str).str.lower().str.strip()
                replaced = s.replace(mapping)
                if not s.equals(replaced):
                    self.df[col] = replaced
                    self.report['categorical_standardizations'][col] = mapping
                    self.logger.debug(f"Standardized '{col}'.")

    def _apply_custom_filters(self) -> None:
        if 'empo_3' in self.df.columns:
            to_remove = ['animal distal gut', 'animal corpus', 'animal secretion']
            mask = self.df['empo_3'].astype(str).str.lower().isin(to_remove)
            if mask.any():
                before = len(self.df)
                self.df = self.df[~mask]
                self.logger.info(f"Filtered {before - len(self.df)} rows on 'empo_3'.")

    def _parse_column_unit(self, col_name: str) -> Tuple[Optional[str], Optional[str]]:
        for unit, pattern in self.DEFAULT_UNIT_PATTERNS.items():
            m = pattern.search(col_name)
            if m:
                return col_name[:m.start()], unit
        return None, None

    def _standardize_units(self) -> None:
        groups: Dict[str, List[Tuple[str, str]]] = {}
        for col in self.df.columns:
            base, unit = self._parse_column_unit(col)
            if base and unit:
                key = next((k for k in self.DEFAULT_MEASUREMENT_STANDARDS if k in base), base)
                groups.setdefault(key, []).append((col, unit))

        for base_name, cols_with_units in groups.items():
            if len(cols_with_units) < 2:
                continue
            target_unit = self.DEFAULT_MEASUREMENT_STANDARDS.get(base_name)
            if not target_unit:
                continue
            target_col = f"{base_name}_{target_unit}"
            self.logger.info(f"Merging {[c[0] for c in cols_with_units]} → '{target_col}'")
            merged = pd.Series(np.nan, index=self.df.index, dtype=float)
            for col_name, unit in cols_with_units:
                src = pd.to_numeric(self.df[col_name], errors='coerce')
                if unit == target_unit:
                    merged.update(src)
                elif unit in self.DEFAULT_CONVERSIONS and self.DEFAULT_CONVERSIONS[unit][0] == target_unit:
                    converted = self.DEFAULT_CONVERSIONS[unit][1](src)
                    merged.update(converted)
                else:
                    self.logger.warning(f"Cannot convert '{unit}' → '{target_unit}' for '{col_name}'. Skipping.")
            self.df[target_col] = merged
            drop_cols = [c for c, _ in cols_with_units]
            self.df.drop(columns=drop_cols, inplace=True)
            self.report['unit_standardizations'][target_col] = drop_cols

    def _collapse_all_suffixes(self) -> None:
        for suffix in self.config.metadata.suffixes_to_collapse or []:
            self._collapse_suffix_columns(suffix)

    def _collapse_suffix_columns(self, suffix: str) -> None:
        suffix_cols = [c for c in self.df.columns if c.endswith(suffix)]
        drop_cols = []
        for col in suffix_cols:
            base = col[:-len(suffix)]
            if base in self.df.columns:
                self.df[base] = self.df[base].combine_first(self.df[col])
                drop_cols.append(col)
            else:
                self.df.rename(columns={col: base}, inplace=True)
        if drop_cols:
            self.df.drop(columns=drop_cols, inplace=True, errors='ignore')

    def _collapse_ph_columns(self) -> None:
        ph_cols = [c for c in self.df.columns if self.PH_PATTERN.match(c) and 'std' not in c]
        if len(ph_cols) > 1:
            if 'ph' not in ph_cols:
                self.df['ph'] = np.nan
                ph_cols.insert(0, 'ph')
            for col in ph_cols:
                self.df['ph'].fillna(pd.to_numeric(self.df[col], errors='coerce'), inplace=True)
            self.df.drop(columns=[c for c in ph_cols if c != 'ph'], inplace=True, errors='ignore')

    def _standardize_column_names(self) -> None:
        self.df.rename(columns=self.config.metadata.mappings or self.DEFAULT_COLUMN_MAPPINGS, inplace=True)

    # ========================================================================
    # Stage 2: Processing & Inference
    # ========================================================================

    def _run_processing_steps(self) -> None:
        self.logger.info("--- Stage 2: Processing and Inference ---")
        self.original_df_for_enrichment = self.df.copy()
        steps = [
            ("Extracting and validating geolocation", self._process_geolocation),
            ("Inferring ontology terms", self._process_ontology),
            ("Processing contamination status", self._process_contamination_status),
            ("Ensuring ENA accession columns exist", self._process_ena_accessions),
            ("Standardizing date formats", self._standardize_dates),
        ]
        self._execute_steps(steps)

    def _process_geolocation(self) -> None:
        initial = len(self.df)
        lat = pd.Series(np.nan, index=self.df.index)
        lon = pd.Series(np.nan, index=self.df.index)

        # Direct lat/lon columns
        for src in [c for c in self.DEFAULT_COORDINATE_SOURCES['lat'] if c in self.df.columns]:
            lat.fillna(pd.to_numeric(self.df[src], errors='coerce'), inplace=True)
        for src in [c for c in self.DEFAULT_COORDINATE_SOURCES['lon'] if c in self.df.columns]:
            lon.fillna(pd.to_numeric(self.df[src], errors='coerce'), inplace=True)

        missing = lat.isna() | lon.isna()
        if missing.any():
            pair_sources = [c for c in self.DEFAULT_COORDINATE_SOURCES['pairs'] if c in self.df.columns]
            if 'location' in self.df.columns and 'location' not in pair_sources:
                pair_sources.append('location')
            if 'lat' in self.df.columns and lat.isna().all():
                if 'lat' not in pair_sources:
                    pair_sources.append('lat')

            for src in pair_sources:
                if not missing.any():
                    break
                to_process = self.df.loc[missing, src].dropna()
                if to_process.empty:
                    continue
                extracted = to_process.astype(str).apply(self._extract_coords_from_string).apply(pd.Series)
                if not extracted.empty:
                    extracted.columns = ['new_lat', 'new_lon']
                    lat.update(extracted['new_lat'])
                    lon.update(extracted['new_lon'])
                    missing = lat.isna() | lon.isna()

        self.df['lat'] = pd.to_numeric(lat, errors='coerce')
        self.df['lon'] = pd.to_numeric(lon, errors='coerce')
        valid = self.df['lat'].between(-90, 90) & self.df['lon'].between(-180, 180)
        if valid.any():
            self.df = self.df[valid].reset_index(drop=True)
            self.logger.info(f"Geolocation: {initial} → {len(self.df)} valid ({initial - len(self.df)} dropped).")
        else:
            self.logger.info("Geolocation: no valid coordinates found; preserving all rows.")

    def _extract_coords_from_string(self, s: str) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(s, str):
            return None, None

        # Pattern: decimal degrees with direction letters
        dd_dir = r'([\d\.-]+(?:[eE][-+]?\d+)?)\s*([NS])\s*([\d\.-]+(?:[eE][-+]?\d+)?)\s*([EW])'
        m = re.search(dd_dir, s, re.IGNORECASE)
        if m:
            try:
                lat = float(m.group(1))
                if m.group(2).upper() == 'S':
                    lat *= -1
                lon = float(m.group(3))
                if m.group(4).upper() == 'W':
                    lon *= -1
                return lat, lon
            except ValueError:
                pass

        # Pattern: simple comma-separated decimal degrees
        dd = r'([-+]?[1-8]?\d(?:\.\d+)?|[-+]?90(?:\.0+)?),\s*([-+]?180(?:\.0+)?|[-+]?(?:1[0-7]\d|[1-9]?\d)(?:\.\d+)?)'
        m = re.search(dd, s)
        if m:
            try:
                return float(m.group(1)), float(m.group(2))
            except (ValueError, IndexError):
                pass

        # Pattern: degrees-minutes-seconds
        if '°' in s:
            parts = re.findall(r'(\d{1,3}(?:[°\.\d\s\'"]*))\s*([NSEW])', s, re.IGNORECASE)
            if len(parts) >= 2:
                lat = self._dms_to_dd(f"{parts[0][0]} {parts[0][1]}")
                lon = self._dms_to_dd(f"{parts[1][0]} {parts[1][1]}")
                if lat is not None and lon is not None:
                    return lat, lon
        return None, None

    def _dms_to_dd(self, dms_str: str) -> Optional[float]:
        dms_str = dms_str.strip().upper()
        try:
            parts = re.split(r'[°\'"]+', dms_str)
            d = float(parts[0])
            m = float(parts[1]) if len(parts) > 1 and parts[1].strip() else 0.0
            s = float(parts[2]) if len(parts) > 2 and parts[2].strip() else 0.0
            dd = d + m / 60.0 + s / 3600.0
            if re.search(r'[SW]', dms_str):
                dd *= -1
            return dd
        except (ValueError, IndexError):
            return None

    def _process_ontology(self) -> None:
        if self.df.empty:
            return
        text_cols = self.df.select_dtypes(include='object').fillna('').astype(str)
        combined = text_cols.agg(' '.join, axis=1).str.lower()
        for term_category, term_map in self.ONTOLOGY_MAP.items():
            if term_category not in self.df.columns or self.df[term_category].isnull().all():
                self.df[term_category] = combined.apply(
                    lambda x: next((cat for cat, kws in term_map.items() if any(kw in x for kw in kws)), 'Unknown')
                )
                self.logger.debug(f"Inferred ontology '{term_category}'.")

    def _process_contamination_status(self) -> None:
        if 'nuclear_contamination_status' in self.df.columns:
            true_vals = ['true', 'yes', '1', 'contaminated']
            self.df['nuclear_contamination_status'] = (
                self.df['nuclear_contamination_status'].astype(str).str.lower().isin(true_vals)
            )
        else:
            self.df['nuclear_contamination_status'] = False

    def _process_ena_accessions(self) -> None:
        for col in ['ena_study_acc', 'ena_sample_acc', 'ena_experiment_acc', 'ena_run_acc']:
            if col not in self.df.columns:
                self.df[col] = 'N/A'
            else:
                self.df[col] = self.df[col].astype('string').fillna('N/A')

    def _standardize_dates(self) -> None:
        for col in [c for c in self.df.columns if 'date' in c.lower() or 'time' in c.lower()]:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # ========================================================================
    # Stage 3: Asynchronous Enrichment
    # ========================================================================

    async def _run_enrichment_steps(self) -> None:
        self.logger.info("--- Stage 3: Enrichment (Async) ---")
        async with aiohttp.ClientSession() as session:
            enricher = MetadataEnricher(
                session=session,
                ncbi_api_key=self.ncbi_api_key,
            )
            if self.config.metadata.enable_geocoding:
                await enricher.enrich_location_from_coords(self.df)
            await enricher.convert_envo_codes(self.df)
            await enricher.find_publications(self.df)

        await self._run_ena_enrichment()

    async def _run_ena_enrichment(self) -> None:
        if not self.config.apis.enabled or not self.config.apis.ena.enabled:
            self.logger.debug("ENA enrichment disabled.")
            return

        ena_email = self.config.credentials.ena_email or self.config.credentials.email
        if not ena_email:
            self.logger.warning("ENA email missing; skipping ENA enrichment.")
            return

        try:
            from .ena import ENAEnrichmentPipeline
            self.logger.info("Starting full ENA metadata enrichment...")
            async with ENAEnrichmentPipeline(self.config) as pipeline:
                enriched = await pipeline.enrich_samples(self.df)
                # Merge all new columns from ENA (including runs, experiments, biosamples, taxonomies)
                for col in enriched.columns:
                    if col in ['run_accession', 'sample_accession', '#sampleid', 'sample_id']:
                        continue  # keep original IDs
                    if col not in self.df.columns:
                        self.df[col] = enriched[col]
                    else:
                        mask = self.df[col].isna()
                        if mask.any():
                            self.df.loc[mask, col] = enriched.loc[mask, col]
            self.logger.info("✅ ENA enrichment completed.")
        except ImportError:
            self.logger.warning("ENA enrichment module not available.")
        except Exception as e:
            self.logger.warning(f"ENA enrichment failed (continuing): {e}")

    # ========================================================================
    # Stage 4: Host categorisation & filtering
    # ========================================================================

    def _categorize_and_filter_host(self) -> None:
        if self.df.empty:
            return

        host_col = 'host' if 'host' in self.df.columns else None
        sci_col = 'scientific_name' if 'scientific_name' in self.df.columns else None

        is_host = pd.Series(False, index=self.df.index)
        if host_col:
            is_host |= self.df[host_col].notna() & (self.df[host_col].astype(str).str.strip() != '')
        if sci_col:
            from .constants import exclusion_keywords
            pattern = '|'.join(exclusion_keywords)
            is_host |= self.df[sci_col].astype(str).str.contains(pattern, case=False, na=False)

        self.df['sample_category'] = is_host.map({True: 'host-associated', False: 'environmental'})

        if self.config.metadata.exclude_host:
            before = len(self.df)
            self.df = self.df[~is_host].copy()
            self.logger.info(f"Host filter: removed {before - len(self.df)} samples, {len(self.df)} remaining.")

    # ========================================================================
    # Utility
    # ========================================================================

    def _execute_steps(self, steps: List[Tuple[str, Callable]]) -> None:
        for name, func in steps:
            try:
                func()
                self.report['actions'].append(name)
            except Exception as e:
                self.logger.error(f"Error in '{name}': {e}", exc_info=True)
                raise

    def get_cleaning_report(self) -> Dict[str, Any]:
        self.report['final_shape'] = self.df.shape
        self.report['summary'] = {
            'rows_initial': self.initial_shape[0],
            'cols_initial': self.initial_shape[1],
            'rows_final': self.df.shape[0],
            'cols_final': self.df.shape[1],
            'rows_removed': self.initial_shape[0] - self.df.shape[0],
            'cols_removed': self.initial_shape[1] - self.df.shape[1],
            'duplicate_rows_removed': self.report.get('duplicate_rows_removed', 0),
        }
        return self.report

    def suggest_categorical_mappings(
        self, similarity_threshold: int = 90, max_unique_values: int = 100
    ) -> Dict[str, Dict[str, str]]:
        """Analyze categorical columns and suggest value harmonization mappings."""
        suggestions = {}
        categorical_cols = self.df.select_dtypes(include=['object', 'category']).columns
        from rich.progress import Progress
        with Progress() as progress:
            task = progress.add_task("Analyzing columns", total=len(categorical_cols))
            for col in categorical_cols:
                try:
                    if not (2 <= self.df[col].nunique() <= max_unique_values):
                        continue
                    vc = self.df[col].astype(str).str.lower().str.strip().value_counts()
                    vals = vc.index.tolist()
                    groups, seen = [], set()
                    for v in vals:
                        if v in seen:
                            continue
                        group = {o for o in vals if fuzz.ratio(v, o) >= similarity_threshold}
                        groups.append(list(group))
                        seen.update(group)
                    mapping = {}
                    for grp in groups:
                        if len(grp) > 1:
                            canon = max(grp, key=lambda x: vc.get(x, 0))
                            for v in grp:
                                if v != canon:
                                    mapping[v] = canon
                    if mapping:
                        suggestions[col] = mapping
                except Exception as e:
                    self.logger.error(f"Error analyzing '{col}': {e}", exc_info=True)
                finally:
                    progress.update(task, advance=1)
        return suggestions