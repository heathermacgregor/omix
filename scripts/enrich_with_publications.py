#!/usr/bin/env python3
"""
Unified pipeline for metadata enrichment with publication discovery.

This combines:
1. Metadata fetching/enrichment (ENA lookup, geocoding, ENVO codes)
2. Publication discovery (multi-source search across 12+ APIs)
3. Publication validation (accession relevance filtering)
4. Metadata integration (merge publications into metadata output)

Usage:
    python scripts/enrich_with_publications.py input.csv -o output.csv --config config.yaml
"""

import asyncio
import json
from pathlib import Path
from typing import Optional, Dict, List
import sys

from omix.config import Config, load_config
from omix.logging_utils import setup_logging, get_logger
from omix.metadata.file_workflow import enrich_metadata_from_path, _load_table, _save_table
from omix.publications.fetcher import PublicationFetcher
from omix.publications.extractors.omics import SixteenSExtractor
from omix.publications.cache import PublicationCache
from omix.publications.apis import (
    CrossrefAPI, EuropePMCAPI, NCBIAPI, SemanticScholarAPI,
    ArxivAPI, BioarxivAPI, CoreAPI, DataciteAPI, DOAJAPI, PLOSAPI,
    UnpaywallAPI, ZenodoAPI,
)
from omix.validators.primer_db import ProbeBaseDatabase

logger = get_logger("omix.enrich_with_publications")


def filter_publications_by_accession(publications: Dict, validated_file: Optional[Path] = None) -> Dict:
    """
    Filter publications to only include those with direct accession matches.
    
    If validated_file exists, use it; otherwise apply inline validation.
    """
    if validated_file and validated_file.exists():
        logger.info(f"Loading pre-validated publications from {validated_file}")
        with validated_file.open() as f:
            return json.load(f)
    
    # Inline validation
    logger.info("Applying inline publication validation...")
    filtered = {}
    for study_accession, pubs in publications.items():
        filtered[study_accession] = []
        for pub in pubs:
            if pub.get('status') != '✓ Extraction complete.':
                continue
            
            # Keep if: (1) accession in matched_queries AND not just DATA query, OR (2) text mentions > 0
            matched_queries = pub.get('matched_queries', [])
            accession_in_text = pub.get('accession_mentions_in_text', 0) > 0
            direct_match = any(
                q == study_accession or (study_accession in q and not q.startswith('DATA:'))
                for q in matched_queries
            )
            
            if direct_match or accession_in_text:
                filtered[study_accession].append(pub)
    
    return filtered


def populate_publication_fields(df, publications: Dict) -> int:
    """Populate publication metadata into dataframe."""
    filled_rows = 0
    studies_with_dois = 0
    
    if 'publication_count' not in df.columns:
        df['publication_count'] = None
    if 'publication_dois' not in df.columns:
        df['publication_dois'] = None
    
    for study_accession, pubs in publications.items():
        # Collect unique DOIs
        dois = []
        seen = set()
        for pub in pubs:
            if not isinstance(pub, dict):
                continue
            doi = pub.get('doi')
            if doi and doi not in seen:
                dois.append(doi)
                seen.add(doi)
        
        if dois or pubs:
            mask = df['study_accession'] == study_accession
            filled_rows += int(mask.sum())
            df.loc[mask, 'publication_count'] = len(dois)
            df.loc[mask, 'publication_dois'] = '; '.join(dois) if dois else ''
            
            if dois:
                studies_with_dois += 1
    
    return filled_rows, studies_with_dois


async def run_full_pipeline(
    input_path: Path,
    output_path: Optional[Path],
    config: Config,
    validate_publications: bool = True,
    api_key: Optional[str] = None,
    no_llm: bool = False,
    builtin_primers: bool = False,
    primer_db: Optional[Path] = None,
    max_rounds: int = 3,
) -> int:
    """
    Run the complete enrichment pipeline: metadata + publications + integration.
    
    Returns:
        Number of rows in enriched output
    """
    
    # ---- PHASE 1: Metadata Enrichment ----
    logger.info(f"\n{'='*70}")
    logger.info("PHASE 1: Metadata Enrichment")
    logger.info(f"{'='*70}")
    
    logger.info(f"Loading {input_path}...")
    metadata = await enrich_metadata_from_path(
        input_path=input_path,
        output_path=None,  # Don't save yet
        config=config,
        enable_ena_lookup=True,
        preserve_all_rows=True,
    )
    
    logger.info(f"✓ Metadata enriched: {len(metadata)} rows × {len(metadata.columns)} columns")
    
    # ---- PHASE 2: Extract Study Accessions ----
    logger.info(f"\n{'='*70}")
    logger.info("PHASE 2: Publication Discovery")
    logger.info(f"{'='*70}")
    
    if 'study_accession' not in metadata.columns:
        logger.error("❌ study_accession column missing from enriched metadata")
        return 0
    
    study_accessions = sorted(set(metadata['study_accession'].dropna().unique()))
    logger.info(f"Found {len(study_accessions)} unique studies")
    
    if not study_accessions:
        logger.warning("⚠️ No study accessions found; skipping publication fetch")
        if output_path:
            _save_table(metadata, output_path)
            logger.info(f"✓ Output saved to {output_path}")
        return len(metadata)
    
    # ---- PHASE 3: Fetch Publications ----
    logger.info(f"Fetching publications for {len(study_accessions)} studies...")
    
    retry_config = {
        'max_retries': config.publication.max_retries,
        'base_delay': config.publication.base_delay_seconds,
        'max_delay': config.publication.max_delay_seconds,
    }
    
    sources = [
        CrossrefAPI(config.credentials.email, **retry_config),
        EuropePMCAPI(config.credentials.email, **retry_config),
        NCBIAPI(config.credentials.email, config.credentials.ncbi_api_key, **retry_config),
        SemanticScholarAPI(config.credentials.email, **retry_config),
        ArxivAPI(config.credentials.email, **retry_config),
        BioarxivAPI(config.credentials.email, **retry_config),
        CoreAPI(config.credentials.email, **retry_config),
        DataciteAPI(config.credentials.email, **retry_config),
        DOAJAPI(config.credentials.email, **retry_config),
        PLOSAPI(config.credentials.email, **retry_config),
        UnpaywallAPI(config.credentials.email, **retry_config),
        ZenodoAPI(config.credentials.email, **retry_config),
    ]
    
    # Build extractor
    llm_key = api_key or config.credentials.llm_api_key
    primer_database = None
    if builtin_primers:
        primer_database = ProbeBaseDatabase(use_builtin=True)
    elif primer_db:
        primer_database = ProbeBaseDatabase(db_path=primer_db)
    
    extractor = SixteenSExtractor(
        api_key=llm_key if not no_llm else "",
        primer_db=primer_database
    )
    
    # Fetch publications
    cache = PublicationCache(config.paths.cache_dir / "publications.db")
    fetcher = PublicationFetcher(config, sources, extractor, cache)
    fetcher.MAX_PUBLICATION_ROUNDS = max_rounds
    
    publications = fetcher.fetch_and_analyze_sync(study_accessions)
    logger.info(f"✓ Publications fetched: {sum(len(p) for p in publications.values())} total")
    
    # ---- PHASE 4: Validate Publications ----
    if validate_publications:
        logger.info(f"\n{'='*70}")
        logger.info("PHASE 3: Publication Validation")
        logger.info(f"{'='*70}")
        
        logger.info("Filtering publications by accession relevance...")
        publications = filter_publications_by_accession(publications)
        
        valid_count = sum(len(p) for p in publications.values())
        logger.info(f"✓ Publications validated: {valid_count} with direct accession matches")
    
    # ---- PHASE 5: Integrate into Metadata ----
    logger.info(f"\n{'='*70}")
    logger.info("PHASE 4: Integration")
    logger.info(f"{'='*70}")
    
    filled_rows, studies_with_dois = populate_publication_fields(metadata, publications)
    logger.info(f"✓ Publication metadata integrated")
    logger.info(f"  Rows updated: {filled_rows}")
    logger.info(f"  Studies with DOIs: {studies_with_dois}")
    
    # ---- PHASE 6: Save Output ----
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _save_table(metadata, output_path)
        logger.info(f"\n✓ Output saved to {output_path}")
        logger.info(f"  Final shape: {len(metadata)} rows × {len(metadata.columns)} columns")
    
    return len(metadata)


def main():
    """CLI entry point."""
    import click
    
    @click.command()
    @click.argument('input_path', type=click.Path(exists=True, path_type=Path))
    @click.option('--output', '-o', type=click.Path(path_type=Path), help='Output file path')
    @click.option('--config', '-c', type=click.Path(exists=True, path_type=Path), help='Config file')
    @click.option('--email', '-e', default=None, help='Email address')
    @click.option('--no-validate', is_flag=True, help='Skip publication validation')
    @click.option('--no-llm', is_flag=True, help='Skip LLM extraction')
    @click.option('--builtin', is_flag=True, help='Use built-in primer DB')
    @click.option('--primer-db', type=click.Path(exists=True, path_type=Path), help='Primer DB path')
    @click.option('--api-key', envvar='OMIX_LLM_API_KEY', help='LLM API key')
    @click.option('--max-rounds', type=int, default=3, help='Max publication rounds')
    def enrich(input_path, output, config, email, no_validate, no_llm, builtin, primer_db, api_key, max_rounds):
        """Enrich metadata with ENA data and publication information."""
        cfg = load_config(config) if config else Config()
        if email:
            cfg.credentials.email = email
        
        setup_logging(cfg.logs_dir)
        
        try:
            row_count = asyncio.run(run_full_pipeline(
                input_path=input_path,
                output_path=output,
                config=cfg,
                validate_publications=not no_validate,
                api_key=api_key,
                no_llm=no_llm,
                builtin_primers=builtin,
                primer_db=primer_db,
                max_rounds=max_rounds,
            ))
            
            click.echo(f"\n✅ Pipeline complete! Enriched {row_count} rows.")
        except Exception as e:
            logger.exception("❌ Pipeline failed")
            click.echo(f"❌ Error: {e}", err=True)
            sys.exit(1)
    
    enrich()


if __name__ == '__main__':
    main()
