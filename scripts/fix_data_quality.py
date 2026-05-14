#!/usr/bin/env python3
"""
Data quality fixes for amplicon metadata enrichment.

Fixes:
1. Move ITS2 from subfragment to amplicon_gene
2. Investigate missing primer data
3. Debug missing publications
4. Merge duplicate columns
5. Implement geospatial/semantic QC
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]

def fix_its2_classification(df):
    """Move ITS2 from subfragment column to amplicon_gene column."""
    print("=" * 60)
    print("FIX 1: ITS2 Classification")
    print("=" * 60)
    
    # Count before fix
    its2_before = (df['subfragment'] == 'ITS2').sum()
    print(f"Before: {its2_before} rows with subfragment='ITS2'")
    
    if its2_before > 0:
        # Move ITS2 to amplicon_gene
        mask = df['subfragment'] == 'ITS2'
        df.loc[mask, 'amplicon_gene'] = 'ITS2'
        df.loc[mask, 'subfragment'] = np.nan
        
        its2_after = (df['amplicon_gene'] == 'ITS2').sum()
        print(f"After: {its2_after} rows with amplicon_gene='ITS2'")
        print(f"✓ Fixed")
    
    return df


def investigate_primer_data(df):
    """Check primer data availability."""
    print("\n" + "=" * 60)
    print("FIX 2: Missing Primer Data Investigation")
    print("=" * 60)
    
    primer_names_filled = df['primer_names'].notna().sum()
    primer_seqs_filled = df['primer_sequences'].notna().sum()
    
    print(f"Current state:")
    print(f"  primer_names: {primer_names_filled} / {len(df)} rows")
    print(f"  primer_sequences: {primer_seqs_filled} / {len(df)} rows")
    print(f"  primer_set: {df['primer_set'].notna().sum()} / {len(df)} rows")
    
    # Check if data is missing in ENA or in pipeline
    # Sample some studies to see what's available
    studies = df['study_accession'].unique()[:3]
    print(f"\nSampling first 3 studies:")
    for study in studies:
        study_rows = df[df['study_accession'] == study]
        print(f"  {study}: {len(study_rows)} rows")
        print(f"    primer_names filled: {study_rows['primer_names'].notna().sum()}")
        print(f"    primer_set: {study_rows['primer_set'].unique()}")


def investigate_publications(df):
    """Check publication data."""
    print("\n" + "=" * 60)
    print("FIX 3: Missing Publications Investigation")
    print("=" * 60)
    
    pub_count_filled = df['publication_count'].notna().sum()
    pub_dois_filled = df['publication_dois'].notna().sum()
    
    print(f"Current state:")
    print(f"  publication_count: {pub_count_filled} / {len(df)} rows")
    print(f"  publication_dois: {pub_dois_filled} / {len(df)} rows")
    
    # List studies that should have been enriched
    print(f"\nStudies in dataset:")
    studies = df['study_accession'].unique()
    print(f"  Total unique studies: {len(studies)}")
    print(f"  First 5: {studies[:5]}")
    
    return studies


def check_duplicate_columns(df):
    """Identify duplicate or related columns."""
    print("\n" + "=" * 60)
    print("FIX 4: Duplicate Columns Investigation")
    print("=" * 60)
    
    all_cols = df.columns.tolist()
    print(f"Total columns: {len(all_cols)}")
    
    # Look for _run and _biosample suffixes
    run_cols = [c for c in all_cols if '_run' in c.lower()]
    biosample_cols = [c for c in all_cols if '_biosample' in c.lower()]
    
    print(f"\nColumns with '_run': {len(run_cols)}")
    if run_cols:
        print(f"  Examples: {run_cols[:5]}")
    
    print(f"\nColumns with '_biosample': {len(biosample_cols)}")
    if biosample_cols:
        print(f"  Examples: {biosample_cols[:5]}")
    
    # Check for duplicate data
    print(f"\nLooking for duplicate fields...")
    print(f"  run_accession: {df['run_accession'].notna().sum()} non-null")
    if 'run_accession_run' in all_cols:
        print(f"  run_accession_run: {df['run_accession_run'].notna().sum()} non-null")


def geospatial_qc(df):
    """Check geospatial data quality."""
    print("\n" + "=" * 60)
    print("FIX 5: Geospatial QC")
    print("=" * 60)
    
    lat_filled = df['lat'].notna().sum()
    lon_filled = df['lon'].notna().sum()
    loc_filled = df['location'].notna().sum()
    
    print(f"Geospatial data availability:")
    print(f"  lat: {lat_filled} / {len(df)}")
    print(f"  lon: {lon_filled} / {len(df)}")
    print(f"  location: {loc_filled} / {len(df)}")
    
    # Check for mismatches
    both_coords = df[df['lat'].notna() & df['lon'].notna()]
    both_coords_with_loc = both_coords[both_coords['location'].notna()]
    print(f"\nRows with both coordinates and location: {len(both_coords_with_loc)}")
    
    # Check lat/lon range validity
    if lat_filled > 0:
        lat_valid = ((df['lat'] >= -90) & (df['lat'] <= 90)).sum()
        lon_valid = ((df['lon'] >= -180) & (df['lon'] <= 180)).sum()
        print(f"\nLatitude range validation:")
        print(f"  Valid range [-90, 90]: {lat_valid} / {lat_filled}")
        print(f"Longitude range validation:")
        print(f"  Valid range [-180, 180]: {lon_valid} / {lon_filled}")
    
    # Sample mismatches
    print(f"\nSample rows with coordinates and location:")
    sample = both_coords_with_loc[['#sampleid', 'location', 'lat', 'lon']].head(3)
    print(sample.to_string(index=False))


def main():
    input_file = Path('tests/fixtures/amplicon_20/metadata_amplicon_20_enriched.csv')
    output_file = Path('tests/fixtures/amplicon_20/metadata_amplicon_20_enriched_fixed.csv')

    input_file = REPO_ROOT / input_file
    output_file = REPO_ROOT / output_file
    
    print(f"Loading: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"Loaded: {df.shape}\n")
    
    # Apply fixes
    df = fix_its2_classification(df)
    investigate_primer_data(df)
    studies = investigate_publications(df)
    check_duplicate_columns(df)
    geospatial_qc(df)
    
    # Save fixed version
    print("\n" + "=" * 60)
    print("Saving fixed version")
    print("=" * 60)
    df.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")
    print(f"Shape: {df.shape}")


if __name__ == '__main__':
    main()
