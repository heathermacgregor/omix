# omix

A Python package that:

- Fetches comprehensive metadata from public databases (ENA, and soon others).
- Enriches coordinates, dates, host/environment categories, and experimental protocols.
- Searches across multiple publication sources (Crossref, Europe PMC, NCBI, Semantic Scholar, etc.).
- Extracts methodology from full‑text using LLMs (optional).
- Validates findings against reference databases (e.g., primer databases for 16S).
- Works for **any omics** via plugins.

## Installation

```bash
pip install omix
# with LLM support:
pip install omix[llm]
```

## Quick Start

### Command Line

```bash
# Enrich a metadata file with ENA data
omix fetch-metadata samples.tsv --email you@example.com

# Fetch publications for one or more accessions
omix fetch-publications PRJNA864623 --omics 16S --api-key $LLM_KEY

# Run the full metadata cleaning and enrichment pipeline
omix run-pipeline metadata.csv -o enriched.csv

# NEW: Unified pipeline (metadata + publications + validation + integration)
omix enrich-with-publications samples.csv -o enriched_complete.csv --config config.yaml
```

### Unified Metadata + Publications Pipeline

The `enrich-with-publications` command provides an end-to-end workflow:

1. **Metadata Enrichment**: Fetches comprehensive data from ENA (sequences, samples, runs)
2. **Publication Discovery**: Searches across 12+ publication APIs (Crossref, EuropePMC, NCBI, Semantic Scholar, arXiv, bioRxiv, CORE, DataCite, DOAJ, PLOS, Unpaywall, Zenodo)
3. **Publication Validation**: Filters to only include publications with direct accession mentions
4. **Integration**: Merges publication counts and DOIs into the enriched metadata

Output includes all ENA metadata fields plus:
- `publication_count`: Number of validated publications per study
- `publication_dois`: Semicolon-separated list of publication DOIs

```bash
# Basic usage
omix enrich-with-publications input.csv -o output.csv

# With debug config for faster testing
omix enrich-with-publications input.csv -o output.csv --config config.debug.yaml

# Skip validation (keep all publications found)
omix enrich-with-publications input.csv -o output.csv --no-validate

# With LLM-based methodology extraction
omix enrich-with-publications input.csv -o output.csv --api-key $LLM_KEY
```

### Python API

```python
from omix import Config
from omix.metadata.file_workflow import enrich_metadata_from_path
import asyncio

config = Config(email="you@example.com")
df = asyncio.run(enrich_metadata_from_path("samples.csv", config=config))
print(df.head())
```

## Configuration

`omix` can be configured via a YAML file:

```yaml
credentials:
  email: "your.email@example.com"
  ena_email: "ena@example.com"
  llm_api_key: "sk-..."
  ncbi_api_key: "..."

apis:
  sequence:
    ena:
      enabled: true
      max_concurrent: 5
      batch_size: 100
      cache_ttl_days: 30
      fetch_phases: true

metadata:
  sample_id_column: "#sampleid"
  exclude_host: false

paths:
  cache_dir: ".cache"
  logs_dir: "logs"
  primer_db: null
```

Pass it with `--config my_config.yaml` or set environment variables like `OMIX_EMAIL`.

## Documentation

Detailed guides are available in [docs/index.rst](docs/index.rst). The docs
tree includes installation, usage, configuration, and API reference pages
backed by Sphinx.

## Testing & Help

Quick commands to verify `omix` locally:

```bash
# Show available commands (concise)
python -m omix.cli help

# Run a lightweight smoke test (no network by default)
python -m omix.cli test --fixture tests/fixtures/amplicon_20/demo_input_3_studies.csv
```

If you want continuous integration, the repository includes a GitHub Actions
workflow (`.github/workflows/ci.yml`) that runs unit tests and the smoke test
on pushes and pull requests.
