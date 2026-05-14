"""
Command‑line interface for omix.

Provides subcommands:
- build-primer-db : download and build a probeBase primer database.
- fetch-metadata  : enrich a metadata file with ENA data.
- fetch-publications : search and analyse publications linked to accessions.
- run-pipeline    : run the full MetadataManager pipeline on a file.
"""

import asyncio
from pathlib import Path
from typing import List, Optional

import click

from omix import __version__
from omix.config import Config, load_config
from omix.logging_utils import setup_logging
from omix.metadata.file_workflow import enrich_metadata_from_path
from omix.metadata.manager import MetadataManager
from omix.publications.apis.ncbi import PMIDSource
from omix.publications.fetcher import PublicationFetcher
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
from omix.publications.cache import PublicationCache
from omix.validators.primer_db import ProbeBaseDatabase


# --------------------------------------------------------------------------- #
#  Common options
# --------------------------------------------------------------------------- #

def _config_option(f):
    """Decorator that adds --config / --email / --cache-dir options to a command."""
    f = click.option(
        "--config", "-c",
        type=click.Path(exists=True, path_type=Path),
        help="Path to YAML configuration file.",
    )(f)
    f = click.option(
        "--email", "-e",
        default=None,
        help="Email address for polite API requests (overrides config).",
    )(f)
    f = click.option(
        "--cache-dir",
        type=click.Path(path_type=Path),
        help="Override the cache directory.",
    )(f)
    return f


def _build_config(config_path: Optional[Path], email: Optional[str], cache_dir: Optional[Path]) -> Config:
    """Build a Config object from CLI arguments and/or a YAML file."""
    if config_path:
        config = load_config(config_path)
    else:
        config = Config()

    if email:
        config.credentials.email = email
    if cache_dir:
        config.paths.cache_dir = cache_dir
        config.cache_dir = cache_dir
    return config


# --------------------------------------------------------------------------- #
#  Main entry point
# --------------------------------------------------------------------------- #

@click.group()
@click.version_option(version=__version__)
def main():
    """omix – a modular toolkit for omics metadata & publication analysis."""
    pass


# --------------------------------------------------------------------------- #
#  build-primer-db
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--csv", type=click.Path(path_type=Path), default=Path("data/probe_data.csv"),
              help="Path to downloaded probeBase CSV (will be downloaded if missing).")
@click.option("--db", type=click.Path(path_type=Path), default=Path("data/primer_data.db"),
              help="Output SQLite database path.")
@_config_option
def build_primer_db(
    csv: Path,
    db: Path,
    config: Optional[Path],
    email: Optional[str],
    cache_dir: Optional[Path],
):
    """Download the probeBase primer list and build a searchable SQLite database."""
    cfg = _build_config(config, email, cache_dir)
    setup_logging(cfg.logs_dir)

    from omix.validators.probebase_builder import import_and_save_database
    success = import_and_save_database(csv, db)
    if not success:
        click.echo("❌ Primer database build failed.", err=True)
        raise click.Abort()
    click.echo(f"✅ Primer database built at {db}")


# --------------------------------------------------------------------------- #
#  fetch-metadata
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("input_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Output file path.")
@click.option("--sample-id-col", default=None, help="Override sample ID column name.")
@click.option("--no-ena", is_flag=True, help="Disable ENA enrichment.")
@click.option("--no-geocode", is_flag=True, help="Disable reverse geocoding.")
@click.option("--preserve-rows", is_flag=True, default=True, help="Keep rows without coordinates.")
@click.option("--omics-type", default=None, help="Filter by omics type (amplicon, metagenomics, transcriptomics, etc.).")
@click.option("--amplicon-gene", default=None, help="Filter by amplicon gene (16S, 18S, ITS, COI, etc.).")
@click.option("--primer-set", default=None, help="Filter by primer set name.")
@click.option("--subfragment", default=None, help="Filter by subfragment/variable region.")
@click.option("--report", "-r", type=click.Path(path_type=Path), help="Save composition report as JSON to this path.")
@_config_option
def fetch_metadata(
    input_path: Path,
    output: Optional[Path],
    sample_id_col: Optional[str],
    no_ena: bool,
    no_geocode: bool,
    preserve_rows: bool,
    omics_type: Optional[str],
    amplicon_gene: Optional[str],
    primer_set: Optional[str],
    subfragment: Optional[str],
    report: Optional[Path],
    config: Optional[Path],
    email: Optional[str],
    cache_dir: Optional[Path],
):
    """Enrich a metadata file with ENA data and generate an omics profile report."""
    cfg = _build_config(config, email, cache_dir)
    if no_geocode:
        cfg.metadata.enable_geocoding = False
    setup_logging(cfg.logs_dir)

    click.echo(f"📁 Loading {input_path} ...")
    result = asyncio.run(
        enrich_metadata_from_path(
    # --------------------------------------------------------------------------- #
    #  test
    # --------------------------------------------------------------------------- #

            input_path=input_path,
            output_path=output,
            config=cfg,
            sample_id_column=sample_id_col,
            enable_ena_lookup=not no_ena,
            preserve_all_rows=preserve_rows,
            omics_type=omics_type,
            amplicon_gene=amplicon_gene,
            primer_set=primer_set,
            subfragment=subfragment,
            report_output=report,
        )
    )
    click.echo(f"✅ Done. Output has {len(result)} rows and {len(result.columns)} columns.")
    if report:
        click.echo(f"📊 Composition report saved to {report}")


# --------------------------------------------------------------------------- #
#  fetch-publications
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("accessions", nargs=-1)
@click.option("--omics", default="16S", help="Omics type (currently only '16S').")
@click.option("--pmid", type=str, default=None, help="PubMed ID for direct lookup.")
@click.option("--api-key", envvar="OMIX_LLM_API_KEY", help="LLM API key for methodology extraction.")
@click.option("--no-llm", is_flag=True, help="Skip LLM extraction (regex only).")
@click.option("--builtin", is_flag=True, help="Use the built‑in primer database (no probeBase needed).")
@click.option("--primer-db", type=click.Path(exists=True, path_type=Path),
              help="Path to a probeBase SQLite primer database.")
@click.option("--max-rounds", type=int, default=3, help="Maximum citation chasing rounds.")
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Save results as JSON.")
@_config_option
def fetch_publications(
    accessions: List[str],
    omics: str,
    pmid: Optional[str],
    api_key: Optional[str],
    no_llm: bool,
    builtin: bool,
    primer_db: Optional[Path],
    max_rounds: int,
    output: Optional[Path],
    config: Optional[Path],
    email: Optional[str],
    cache_dir: Optional[Path],
):
    """Search and analyse publications for one or more accessions."""
    cfg = _build_config(config, email, cache_dir)
    setup_logging(cfg.logs_dir)

    if not accessions:
        click.echo("❌ You must provide at least one accession.", err=True)
        raise click.Abort()

    # ---- Build publication sources (free and reliable ones) ----
    # FEATURE 3: Pass retry config from publication config
    retry_config = {
        'max_retries': cfg.publication.max_retries,
        'base_delay': cfg.publication.base_delay_seconds,
        'max_delay': cfg.publication.max_delay_seconds,
    }
    sources = [
        CrossrefAPI(cfg.credentials.email, **retry_config),
        EuropePMCAPI(cfg.credentials.email, **retry_config),
        NCBIAPI(cfg.credentials.email, cfg.credentials.ncbi_api_key, **retry_config),
        SemanticScholarAPI(cfg.credentials.email, **retry_config),
        ArxivAPI(cfg.credentials.email, **retry_config),

        BioarxivAPI(cfg.credentials.email, **retry_config),
        CoreAPI(cfg.credentials.email, **retry_config),
        DataciteAPI(cfg.credentials.email, **retry_config),
        DOAJAPI(cfg.credentials.email, **retry_config),
        PLOSAPI(cfg.credentials.email, **retry_config),
        UnpaywallAPI(cfg.credentials.email, **retry_config),
        ZenodoAPI(cfg.credentials.email, **retry_config),
    ]

    # ---- Build omics extractor ----
    llm_key = api_key or cfg.credentials.llm_api_key

    # Primer database: prefer builtin, then external file, else None
    primer_database = None
    if builtin:
        primer_database = ProbeBaseDatabase(use_builtin=True)
    elif primer_db:
        primer_database = ProbeBaseDatabase(db_path=primer_db)

    if pmid:
        sources = [PMIDSource(cfg.credentials.email, pmid, **retry_config)]
    
    if omics.lower() == "16s":
        if no_llm:
            extractor = SixteenSExtractor(api_key="", primer_db=primer_database)
        else:
            extractor = SixteenSExtractor(api_key=llm_key or "", primer_db=primer_database)
    else:
        click.echo(f"❌ Unknown omics type: {omics}", err=True)
        raise click.Abort()

    # ---- Run fetcher ----
    cache = PublicationCache(cfg.paths.cache_dir / "publications.db")
    fetcher = PublicationFetcher(cfg, sources, extractor, cache)
    fetcher.MAX_PUBLICATION_ROUNDS = max_rounds

    click.echo(f"🔍 Searching publications for {len(accessions)} accessions...")
    results = fetcher.fetch_and_analyze_sync(accessions)

    # ---- Output ----
    if output:
        import json
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        click.echo(f"📄 Results written to {output}")
    else:
        for acc, pubs in results.items():
            click.echo(f"\n📌 {acc} – {len(pubs)} publications")
            for pub in pubs[:5]:
                title = (pub.get('publication_title') or 'N/A')[:80]
                status = pub.get('status', '?')
                click.echo(f"   [{status}] {title}")
            if len(pubs) > 5:
                click.echo(f"   … and {len(pubs) - 5} more.")


# --------------------------------------------------------------------------- #
#  run-pipeline
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("input_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Output file path.")
@click.option("--sample-id-col", default=None, help="Override sample ID column name.")
@click.option("--no-geocode", is_flag=True, help="Disable reverse geocoding.")
@_config_option
def run_pipeline(
    input_path: Path,
    output: Optional[Path],
    sample_id_col: Optional[str],
    no_geocode: bool,
    config: Optional[Path],
    email: Optional[str],
    cache_dir: Optional[Path],
):
    """Run the full MetadataManager pipeline on a file (cleaning + enrichment)."""
    cfg = _build_config(config, email, cache_dir)
    if no_geocode:
        cfg.metadata.enable_geocoding = False
    setup_logging(cfg.logs_dir)

    import pandas as pd
    from omix.metadata.file_workflow import _load_table, _save_table, _ensure_sample_id_column

    df = _load_table(input_path)
    if sample_id_col:
        df = _ensure_sample_id_column(df, sample_id_col)
    elif cfg.metadata.sample_id_column not in df.columns:
        df = _ensure_sample_id_column(df, cfg.metadata.sample_id_column)

    manager = MetadataManager(df, cfg, sample_id_column=sample_id_col)
    enriched = asyncio.run(manager.run_pipeline())

    if output:
        _save_table(enriched, output)
        click.echo(f"✅ Enriched metadata written to {output}")
    else:
        click.echo(f"✅ Pipeline complete – {len(enriched)} rows, {len(enriched.columns)} columns.")
        click.echo(enriched.head())


# --------------------------------------------------------------------------- #
#  enrich-with-publications
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("input_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Output file path.")
@click.option("--no-validate", is_flag=True, help="Skip publication validation (keep all publications).")
@click.option("--api-key", envvar="OMIX_LLM_API_KEY", help="LLM API key for methodology extraction.")
@click.option("--no-llm", is_flag=True, help="Skip LLM extraction (regex only).")
@click.option("--builtin", is_flag=True, help="Use the built‑in primer database.")
@click.option("--primer-db", type=click.Path(exists=True, path_type=Path),
              help="Path to a probeBase SQLite primer database.")
@click.option("--max-rounds", type=int, default=3, help="Maximum citation chasing rounds.")
@_config_option
def enrich_with_publications(
    input_path: Path,
    output: Optional[Path],
    no_validate: bool,
    api_key: Optional[str],
    no_llm: bool,
    builtin: bool,
    primer_db: Optional[Path],
    max_rounds: int,
    config: Optional[Path],
    email: Optional[str],
    cache_dir: Optional[Path],
):
    """
    Enrich metadata with ENA data AND publication information in one pipeline.
    
    This unified command:
    1. Fetches and enriches metadata from ENA
    2. Discovers publications from 12+ sources
    3. Validates publications for accession relevance
    4. Integrates publication counts and DOIs into metadata
    
    Output includes: all ENA metadata fields + publication_count + publication_dois
    """
    cfg = _build_config(config, email, cache_dir)
    setup_logging(cfg.logs_dir)
    
    # Import the unified pipeline
    from omix.metadata.file_workflow import _load_table, _save_table
    import pandas as pd
    
    async def async_enrich():
        """Run async enrichment pipeline."""
        from omix.metadata.file_workflow import enrich_metadata_from_path
        from omix.publications.fetcher import PublicationFetcher
        from omix.publications.extractors.omics import SixteenSExtractor
        from omix.publications.cache import PublicationCache
        from omix.publications.apis import (
            CrossrefAPI, EuropePMCAPI, NCBIAPI, SemanticScholarAPI,
            ArxivAPI, BioarxivAPI, CoreAPI, DataciteAPI, DOAJAPI, PLOSAPI,
            UnpaywallAPI, ZenodoAPI,
        )
        
        # ---- Phase 1: Metadata ----
        click.echo("📊 Phase 1: Metadata enrichment...")
        metadata = await enrich_metadata_from_path(
            input_path=input_path,
            output_path=None,
            config=cfg,
            enable_ena_lookup=True,
            preserve_all_rows=True,
        )
        click.echo(f"   ✓ {len(metadata)} rows × {len(metadata.columns)} columns")
        
        # Extract study accessions
        if 'study_accession' not in metadata.columns:
            click.echo("❌ study_accession column missing; cannot fetch publications", err=True)
            if output:
                _save_table(metadata, output)
            return metadata
        
        study_accessions = sorted(set(metadata['study_accession'].dropna().unique()))
        
        if not study_accessions:
            click.echo("⚠️  No study accessions found; skipping publication fetch")
            if output:
                _save_table(metadata, output)
            return metadata
        
        # ---- Phase 2: Publications ----
        click.echo(f"📚 Phase 2: Publication discovery ({len(study_accessions)} studies)...")
        
        retry_config = {
            'max_retries': cfg.publication.max_retries,
            'base_delay': cfg.publication.base_delay_seconds,
            'max_delay': cfg.publication.max_delay_seconds,
        }
        sources = [
            CrossrefAPI(cfg.credentials.email, **retry_config),
            EuropePMCAPI(cfg.credentials.email, **retry_config),
            NCBIAPI(cfg.credentials.email, cfg.credentials.ncbi_api_key, **retry_config),
            SemanticScholarAPI(cfg.credentials.email, **retry_config),
            ArxivAPI(cfg.credentials.email, **retry_config),
            BioarxivAPI(cfg.credentials.email, **retry_config),
            CoreAPI(cfg.credentials.email, **retry_config),
            DataciteAPI(cfg.credentials.email, **retry_config),
            DOAJAPI(cfg.credentials.email, **retry_config),
            PLOSAPI(cfg.credentials.email, **retry_config),
            UnpaywallAPI(cfg.credentials.email, **retry_config),
            ZenodoAPI(cfg.credentials.email, **retry_config),
        ]
        
        llm_key = api_key or cfg.credentials.llm_api_key
        primer_database = None
        if builtin:
            primer_database = ProbeBaseDatabase(use_builtin=True)
        elif primer_db:
            primer_database = ProbeBaseDatabase(db_path=primer_db)
        
        extractor = SixteenSExtractor(api_key=llm_key if not no_llm else "", primer_db=primer_database)
        cache = PublicationCache(cfg.paths.cache_dir / "publications.db")
        fetcher = PublicationFetcher(cfg, sources, extractor, cache)
        fetcher.MAX_PUBLICATION_ROUNDS = max_rounds
        
        publications = fetcher.fetch_and_analyze_sync(study_accessions)
        total_pubs = sum(len(p) for p in publications.values())
        click.echo(f"   ✓ {total_pubs} publications found")
        
        # ---- Phase 3: Validation ----
        if not no_validate:
            click.echo("🔍 Phase 3: Publication validation...")
            
            # Inline validation
            filtered = {}
            for study_accession, pubs in publications.items():
                filtered[study_accession] = []
                for pub in pubs:
                    if pub.get('status') != '✓ Extraction complete.':
                        continue
                    matched_queries = pub.get('matched_queries', [])
                    accession_in_text = pub.get('accession_mentions_in_text', 0) > 0
                    direct_match = any(
                        q == study_accession or (study_accession in q and not q.startswith('DATA:'))
                        for q in matched_queries
                    )
                    if direct_match or accession_in_text:
                        filtered[study_accession].append(pub)
            
            publications = filtered
            valid_count = sum(len(p) for p in publications.values())
            click.echo(f"   ✓ {valid_count} publications with direct accession matches")
        
        # ---- Phase 4: Integration ----
        click.echo("🔗 Phase 4: Integration...")
        
        if 'publication_count' not in metadata.columns:
            metadata['publication_count'] = None
        if 'publication_dois' not in metadata.columns:
            metadata['publication_dois'] = None
        
        filled_rows = 0
        studies_with_dois = 0
        
        for study_accession, pubs in publications.items():
            dois = []
            seen = set()
            for pub in pubs:
                if not isinstance(pub, dict):
                    continue
                doi = pub.get('doi')
                if doi and doi not in seen:
                    dois.append(doi)
                    seen.add(doi)
            
            mask = metadata['study_accession'] == study_accession
            filled_rows += int(mask.sum())
            metadata.loc[mask, 'publication_count'] = len(dois)
            metadata.loc[mask, 'publication_dois'] = '; '.join(dois) if dois else ''
            
            if dois:
                studies_with_dois += 1
        
        click.echo(f"   ✓ {filled_rows} rows updated, {studies_with_dois} studies with DOIs")
        
        return metadata
    
    try:
        enriched_metadata = asyncio.run(async_enrich())
        
        if output:
            output.parent.mkdir(parents=True, exist_ok=True)
            _save_table(enriched_metadata, output)
            click.echo(f"\n✅ Output saved to {output}")
        
        click.echo(f"✅ Complete! {len(enriched_metadata)} rows × {len(enriched_metadata.columns)} columns")
    except Exception as e:
        click.echo(f"\n❌ Error: {e}", err=True)
        raise click.Abort()


# --------------------------------------------------------------------------- #
#  help (friendly listing)
# --------------------------------------------------------------------------- #


@main.command("help")
@click.pass_context
def help_cmd(ctx: click.Context):
    """Show a concise list of available `omix` commands."""
    click.echo("omix — available commands:\n")
    for name, cmd in main.commands.items():
        # skip hidden/implicit commands
        if getattr(cmd, "hidden", False):
            continue
        summary = (cmd.help or "").splitlines()[0] if cmd.help else ""
        click.echo(f"- {name}: {summary}")


# --------------------------------------------------------------------------- #
#  test (smoke test)
# --------------------------------------------------------------------------- #


@main.command()
@click.option("--fixture", type=click.Path(exists=True, path_type=Path),
              default=Path("tests/fixtures/amplicon_20/demo_input_3_studies.csv"),
              help="Path to a small fixture CSV to run the smoke pipeline against.")
@_config_option
def test(fixture: Path, config: Optional[Path], email: Optional[str], cache_dir: Optional[Path]):
    """Run a lightweight smoke test to verify core `omix` functionality.

    This test performs non-network checks and runs the local enrichment pipeline
    with ENA lookups disabled so it can be executed offline.
    """
    cfg = _build_config(config, email, cache_dir)
    setup_logging(cfg.logs_dir)

    click.echo("🔧 Running smoke tests...")

    # 1) basic imports and version
    try:
        click.echo(f"• omix version: {__version__}")
    except Exception as e:
        click.echo(f"✖ Failed to read version: {e}", err=True)
        raise click.Abort()

    # 2) Config sanity
    try:
        _ = cfg.metadata
        click.echo("• Config loaded: OK")
    except Exception as e:
        click.echo(f"✖ Config load failed: {e}", err=True)
        raise click.Abort()

    # 3) Primer DB builtin check
    try:
        from omix.validators.primer_db import ProbeBaseDatabase
        _ = ProbeBaseDatabase(use_builtin=True)
        click.echo("• Built-in primer DB: OK")
    except Exception:
        # non-fatal: warn only
        click.echo("• Built-in primer DB: unavailable (continuing)")

    # 4) Run lightweight enrichment on fixture with ENA disabled
    try:
        import asyncio
        from omix.metadata.file_workflow import enrich_metadata_from_path

        click.echo(f"• Loading fixture: {fixture}")
        df = asyncio.run(
            enrich_metadata_from_path(
                input_path=fixture,
                output_path=None,
                config=cfg,
                enable_ena_lookup=False,
                preserve_all_rows=True,
                save_output=False,
            )
        )
        click.echo(f"• Enrichment result: {len(df)} rows × {len(df.columns)} columns")
    except Exception as e:
        click.echo(f"✖ Smoke enrichment failed: {e}", err=True)
        raise click.Abort()

    click.echo("✅ Smoke tests passed.")


if __name__ == "__main__":
    main()