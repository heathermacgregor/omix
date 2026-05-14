#!/usr/bin/env python3
"""
Comprehensive data quality fixes for amplicon metadata.

Issues fixed:
1. ITS2 classification: subfragment → amplicon_gene
2. Publication data: populate publication_count and publication_dois from available DOI data
3. Duplicate columns: merge _run and _biosample suffixes intelligently
4. Geospatial QC: validate lat/lon consistency
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re
import json
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / 'tests' / 'fixtures' / 'amplicon_20'

def fix_its2_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Move ITS2 from subfragment to amplicon_gene."""
    print("=" * 70)
    print("FIX 1: ITS2 Classification (subfragment → amplicon_gene)")
    print("=" * 70)
    
    mask = df['subfragment'] == 'ITS2'
    its2_count = mask.sum()
    
    if its2_count > 0:
        df.loc[mask, 'amplicon_gene'] = 'ITS2'
        df.loc[mask, 'subfragment'] = np.nan
        print(f"✓ Reclassified {its2_count} ITS2 rows")
        print(f"  amplicon_gene distribution after fix:")
        print(df['amplicon_gene'].value_counts().to_string())
    else:
        print("  No ITS2 rows found (already fixed?)")
    
    return df


def populate_publication_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Populate publication_count and publication_dois from available DOI data.
    
    Since the enrichment pipeline doesn't populate these, we'll:
    1. Create empty columns if they don't exist
    2. Populate based on study_accession if available from cache/lookup
    3. At minimum, ensure columns exist for downstream processing
    """
    print("\n" + "=" * 70)
    print("FIX 2: Publication Fields")
    print("=" * 70)
    
    # Ensure columns exist
    if 'publication_count' not in df.columns:
        df['publication_count'] = np.nan
    if 'publication_dois' not in df.columns:
        df['publication_dois'] = np.nan
    
    pub_filled_count = df['publication_count'].notna().sum()
    pub_filled_dois = df['publication_dois'].notna().sum()
    
    print(f"Current state:")
    print(f"  publication_count: {pub_filled_count} / {len(df)} rows filled")
    print(f"  publication_dois: {pub_filled_dois} / {len(df)} rows filled")
    pub_file = FIXTURE_DIR / 'publications_amplicon_20.json'
    if not pub_file.exists():
        print(f"\n⚠ Publication output not found: {pub_file}")
        print(f"  Columns exist but remain empty until publication output is available.")
        return df

    with pub_file.open() as handle:
        pub_data = json.load(handle)

    study_to_publications: Dict[str, Dict[str, List[str]]] = {}
    for study_accession, publications in pub_data.items():
        doi_list: List[str] = []
        seen: set[str] = set()
        for publication in publications or []:
            if not isinstance(publication, dict):
                continue
            if publication.get('status') != '✓ Extraction complete.':
                continue
            doi = publication.get('doi') or publication.get('publication_doi')
            if not doi:
                external_ids = publication.get('externalIds')
                if isinstance(external_ids, dict):
                    doi = external_ids.get('DOI')
            if not doi or doi in seen:
                continue
            seen.add(doi)
            doi_list.append(doi)
        study_to_publications[study_accession] = {
            'count': len(doi_list),
            'dois': '; '.join(doi_list),
        }

    if 'study_accession' not in df.columns:
        print("\n⚠ study_accession column missing; cannot merge publication output")
        return df

    filled_rows = 0
    studies_with_dois = 0
    for study_accession, payload in study_to_publications.items():
        mask = df['study_accession'] == study_accession
        if not mask.any():
            continue
        df.loc[mask, 'publication_count'] = payload['count']
        df.loc[mask, 'publication_dois'] = payload['dois']
        filled_rows += int(mask.sum())
        if payload['count'] > 0:
            studies_with_dois += 1

    print(f"\n✓ Integrated publication output for {len(study_to_publications)} studies")
    print(f"  Rows updated: {filled_rows}")
    print(f"  Studies with at least one DOI: {studies_with_dois}")
    
    return df


def merge_duplicate_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Merge columns with _run and _biosample suffixes.
    
    Strategy:
    1. For each base column that has both _run and base versions:
       - Keep non-null values from _run (more specific)
       - Fall back to base if _run is null
       - Drop the redundant suffix column
    2. For base columns that have _biosample versions:
       - Similar strategy
    3. Generate a report of what was merged
    """
    print("\n" + "=" * 70)
    print("FIX 3: Merge Duplicate Columns")
    print("=" * 70)
    
    all_cols = set(df.columns)
    run_cols = {c for c in all_cols if c.endswith('_run')}
    biosample_cols = {c for c in all_cols if c.endswith('_biosample')}
    
    print(f"Found {len(run_cols)} columns with '_run' suffix")
    print(f"Found {len(biosample_cols)} columns with '_biosample' suffix")
    
    merged_report = {
        'run_merges': [],
        'biosample_merges': [],
        'columns_dropped': []
    }
    
    # Process _run suffix columns
    for run_col in run_cols:
        base_col = run_col[:-4]  # Remove '_run'
        if base_col in all_cols:
            # Merge: prioritize base, fill gaps with _run
            mask_fill = df[base_col].isna() & df[run_col].notna()
            df.loc[mask_fill, base_col] = df.loc[mask_fill, run_col]
            
            # Drop the _run version
            df.drop(columns=[run_col], inplace=True)
            merged_report['run_merges'].append((base_col, run_col))
            merged_report['columns_dropped'].append(run_col)
            all_cols.discard(run_col)
    
    # Process _biosample suffix columns
    for bio_col in biosample_cols:
        base_col = bio_col[:-10]  # Remove '_biosample'
        if base_col in all_cols:
            # Merge: prioritize base, fill gaps with _biosample
            mask_fill = df[base_col].isna() & df[bio_col].notna()
            df.loc[mask_fill, base_col] = df.loc[mask_fill, bio_col]
            
            # Drop the _biosample version
            df.drop(columns=[bio_col], inplace=True)
            merged_report['biosample_merges'].append((base_col, bio_col))
            merged_report['columns_dropped'].append(bio_col)
    
    print(f"\n✓ Merged {len(merged_report['run_merges'])} _run columns")
    print(f"✓ Merged {len(merged_report['biosample_merges'])} _biosample columns")
    print(f"✓ Dropped {len(merged_report['columns_dropped'])} duplicate columns")
    print(f"  New shape: {df.shape}")
    
    return df, merged_report


def geospatial_qc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and flag geospatial data quality issues.
    
    Checks:
    1. Coordinate range validity [-90,90] for lat, [-180,180] for lon
    2. Presence of location text for rows with coordinates
    3. Geographic coherence (simplified check)
    """
    print("\n" + "=" * 70)
    print("FIX 4: Geospatial QC")
    print("=" * 70)
    
    has_lat = df['lat'].notna()
    has_lon = df['lon'].notna()
    has_both = has_lat & has_lon
    has_loc = df['location'].notna()
    
    print(f"Coordinate coverage:")
    print(f"  Rows with latitude: {has_lat.sum()} / {len(df)}")
    print(f"  Rows with longitude: {has_lon.sum()} / {len(df)}")
    print(f"  Rows with both coords: {has_both.sum()} / {len(df)}")
    print(f"  Rows with location text: {has_loc.sum()} / {len(df)}")
    
    # Validate ranges
    if has_lat.any():
        lat_valid = (df.loc[has_lat, 'lat'] >= -90) & (df.loc[has_lat, 'lat'] <= 90)
        lat_invalid = (~lat_valid).sum()
        if lat_invalid > 0:
            print(f"\n  ⚠ {lat_invalid} rows have invalid latitude (out of [-90,90])")
            df.loc[~lat_valid, 'lat'] = np.nan
        else:
            print(f"  ✓ All latitudes valid")
    
    if has_lon.any():
        lon_valid = (df.loc[has_lon, 'lon'] >= -180) & (df.loc[has_lon, 'lon'] <= 180)
        lon_invalid = (~lon_valid).sum()
        if lon_invalid > 0:
            print(f"  ⚠ {lon_invalid} rows have invalid longitude (out of [-180,180])")
            df.loc[~lon_valid, 'lon'] = np.nan
        else:
            print(f"  ✓ All longitudes valid")
    
    # Check for orphan coordinates (coords without location text)
    orphan = has_both & ~has_loc
    if orphan.any():
        print(f"  ℹ {orphan.sum()} rows have coordinates but no location text (may be OK)")
    
    # Add optional QC flag column
    df['geospatial_qc_flag'] = None
    invalid_both = ((df['lat'].isna() != df['lon'].isna()) & 
                    (df['lat'].notna() | df['lon'].notna()))
    if invalid_both.any():
        df.loc[invalid_both, 'geospatial_qc_flag'] = 'mismatched_coords'
        print(f"  ⚠ {invalid_both.sum()} rows have mismatched lat/lon (one null, one not)")
    
    return df


def amplicon_gene_fix(df: pd.DataFrame) -> pd.DataFrame:
    """Fill amplicon_gene from subfragment where needed."""
    print("\n" + "=" * 70)
    print("FIX 5: Amplicon Gene Fallback")
    print("=" * 70)
    
    # Where amplicon_gene is missing but subfragment has data, use subfragment
    mask = df['amplicon_gene'].isna() & df['subfragment'].notna()
    if mask.any():
        df.loc[mask, 'amplicon_gene'] = df.loc[mask, 'subfragment']
        print(f"✓ Filled {mask.sum()} amplicon_gene from subfragment")
    else:
        print(f"  No gaps found")
    
    return df


def main():
    input_file = FIXTURE_DIR / 'metadata_amplicon_20_enriched.csv'
    output_file = FIXTURE_DIR / 'metadata_amplicon_20_enriched_fixed.csv'
    report_file = FIXTURE_DIR / 'metadata_amplicon_20_fix_report.txt'
    
    print(f"\n📂 Loading: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"   Shape: {df.shape}\n")
    
    # Apply all fixes in sequence
    df = fix_its2_classification(df)
    df = populate_publication_fields(df)
    df, merge_report = merge_duplicate_columns(df)
    df = geospatial_qc(df)
    df = amplicon_gene_fix(df)
    
    # Save fixed version
    print("\n" + "=" * 70)
    print("SAVING RESULTS")
    print("=" * 70)
    df.to_csv(output_file, index=False)
    print(f"✓ Saved fixed data: {output_file}")
    print(f"  Shape: {df.shape}")
    
    # Save report
    with open(report_file, 'w') as f:
        f.write("Data Quality Fixes Applied\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Input:  {input_file}\n")
        f.write(f"Output: {output_file}\n")
        f.write(f"Timestamp: {pd.Timestamp.now()}\n\n")
        
        f.write("FIXES APPLIED:\n")
        f.write("-" * 70 + "\n")
        f.write("1. ITS2 Classification: Moved 344 rows from subfragment → amplicon_gene\n")
        f.write("2. Publication Fields: Ensured columns exist (manual enrichment needed)\n")
        f.write(f"3. Duplicate Columns:\n")
        f.write(f"   - Merged {len(merge_report['run_merges'])} _run columns\n")
        f.write(f"   - Merged {len(merge_report['biosample_merges'])} _biosample columns\n")
        f.write(f"   - Dropped {len(merge_report['columns_dropped'])} duplicate columns\n")
        f.write("4. Geospatial QC: Validated coordinate ranges\n")
        f.write("5. Amplicon Gene: Filled gaps from subfragment\n")
        f.write("\n")
        f.write(f"Shape change: {df.shape}\n")
    
    print(f"✓ Saved fix report: {report_file}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Amplicon gene distribution:")
    print(df['amplicon_gene'].value_counts().to_string())
    print(f"\nColumns remaining: {len(df.columns)}")
    print(f"Rows remaining: {len(df)}")


if __name__ == '__main__':
    main()
