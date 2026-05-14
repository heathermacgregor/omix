#!/usr/bin/env python3
"""
Validate and filter publications to ensure accession relevance.

This script ensures that only publications with direct accession mentions
are included in the enriched metadata output.

Validation strategy:
1. Publications found via direct accession match (accession in matched_queries and not just DATA query)
2. Publications with accession mentions in text (accession_mentions_in_text > 0)
3. Filters out generic DATA query matches that don't show study-specific relevance
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "amplicon_20"


def has_direct_accession_match(pub: Dict, accession: str) -> bool:
    """
    Check if publication has a direct accession match in queries.
    
    Returns True if:
    - Accession appears directly in matched_queries, OR
    - Accession is found in text (accession_mentions_in_text > 0)
    
    Returns False if:
    - Only generic DATA:"{accession}" query match (not study-specific)
    """
    matched_queries = pub.get('matched_queries', [])
    
    # Check for accession mentions in text
    if pub.get('accession_mentions_in_text', 0) > 0:
        return True
    
    # Check for direct accession in queries (exact match, not DATA query)
    for query in matched_queries:
        if query == accession or (accession in query and not query.startswith('DATA:')):
            return True
    
    return False


def validate_publications(pub_file: Path) -> Tuple[Dict, Dict]:
    """
    Load publications and validate accession relevance.
    
    Returns:
        (filtered_publications, validation_report)
    """
    with pub_file.open() as f:
        pub_data = json.load(f)
    
    filtered_pubs = {}
    report = {
        'total_studies': 0,
        'total_publications_before': 0,
        'total_publications_after': 0,
        'studies_with_valid_pubs': 0,
        'detail_by_study': {}
    }
    
    for study_accession, publications in pub_data.items():
        filtered_pubs[study_accession] = []
        study_report = {
            'total_before': len(publications),
            'total_after': 0,
            'filtered_out': [],
            'kept': []
        }
        
        report['total_studies'] += 1
        report['total_publications_before'] += len(publications)
        
        for pub in publications:
            if pub.get('status') != '✓ Extraction complete.':
                continue
            
            if has_direct_accession_match(pub, study_accession):
                filtered_pubs[study_accession].append(pub)
                study_report['total_after'] += 1
                study_report['kept'].append({
                    'doi': pub.get('doi'),
                    'title': pub.get('publication_title', 'N/A')[:80],
                    'reason': 'Direct accession match' if study_accession in pub.get('matched_queries', []) else 'Text mentions',
                })
                report['total_publications_after'] += 1
            else:
                study_report['filtered_out'].append({
                    'doi': pub.get('doi'),
                    'title': pub.get('publication_title', 'N/A')[:80],
                    'matched_queries': pub.get('matched_queries', []),
                })
        
        if study_report['total_after'] > 0:
            report['studies_with_valid_pubs'] += 1
        
        report['detail_by_study'][study_accession] = study_report
    
    return filtered_pubs, report


def save_filtered_publications(filtered_pubs: Dict, output_file: Path):
    """Save filtered publications to JSON file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
        json.dump(filtered_pubs, f, indent=2, default=str)


def print_validation_report(report: Dict):
    """Print validation report to stdout."""
    print("\n" + "=" * 70)
    print("PUBLICATION VALIDATION REPORT")
    print("=" * 70)
    
    print(f"\nSummary:")
    print(f"  Total studies: {report['total_studies']}")
    print(f"  Publications before filtering: {report['total_publications_before']}")
    print(f"  Publications after filtering: {report['total_publications_after']}")
    print(f"  Filtered out: {report['total_publications_before'] - report['total_publications_after']}")
    print(f"  Studies with at least 1 valid publication: {report['studies_with_valid_pubs']}")
    
    print(f"\nFiltering details by study:")
    for study_accession in sorted(report['detail_by_study'].keys()):
        detail = report['detail_by_study'][study_accession]
        filtered_count = detail['total_before'] - detail['total_after']
        print(f"\n  {study_accession}:")
        print(f"    Before: {detail['total_before']} | After: {detail['total_after']} | Filtered: {filtered_count}")
        
        if detail['kept']:
            print(f"    ✓ Kept: {detail['total_after']}")
            for pub in detail['kept'][:2]:
                print(f"      - {pub['title']} [{pub['reason']}]")
            if len(detail['kept']) > 2:
                print(f"      ... and {len(detail['kept']) - 2} more")
        
        if detail['filtered_out']:
            print(f"    ✗ Filtered: {len(detail['filtered_out'])}")
            for pub in detail['filtered_out'][:2]:
                print(f"      - {pub['title'][:60]} (queries: {pub['matched_queries']})")
            if len(detail['filtered_out']) > 2:
                print(f"      ... and {len(detail['filtered_out']) - 2} more")
    
    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    import sys
    
    input_file = FIXTURE_DIR / 'publications_amplicon_20.json'
    output_file = FIXTURE_DIR / 'publications_amplicon_20_validated.json'
    
    if not input_file.exists():
        print(f"❌ Input file not found: {input_file}")
        sys.exit(1)
    
    print(f"📁 Loading publications from {input_file}...")
    filtered_pubs, report = validate_publications(input_file)
    
    print_validation_report(report)
    
    print(f"\n💾 Saving validated publications to {output_file}...")
    save_filtered_publications(filtered_pubs, output_file)
    print(f"✅ Done!")


if __name__ == '__main__':
    main()
