# Contributing to omix

Thanks for taking the time to improve `omix`.

## Development Setup

1. Clone the repository and enter the workspace.
2. Create or activate a Python environment. This repo already includes a local virtual environment at `omix_env/`.
3. Install the package in editable mode with development dependencies:

```bash
pip install -e .[dev]
```

4. If you work on the publication pipeline, make sure `config.debug.yaml` is available. It is the preferred test config because it contains debug-friendly settings and the credentials used by the local test data.

## Running Tests

Run the full test suite:


Large test fixtures
-------------------

Some fixtures in `tests/fixtures/` are large and can bloat the repository. We
recommend using Git LFS for files larger than ~50MB. A recommended `.gitattributes`
entry has been added to the repository for `tests/fixtures/amplicon_20/metadata_amp_only.csv`.

If you do not use Git LFS, consider trimming or replacing large fixtures with
smaller samples before committing.
```bash
python -m pytest
```

Run the fast unit tests only:

```bash
python -m pytest tests/unit -q
```

Run the integration tests that do not require live services:

```bash
python -m pytest tests/integration -q
```

Run the live integration checks only when you explicitly want network-backed tests:

```bash
OMIX_RUN_INTEGRATION=1 python -m pytest tests/integration -q
```

For publication-related work, add or update tests in `tests/unit/` whenever possible. Prefer deterministic fixtures over live API calls.

## What Good Changes Look Like

- Keep edits focused and minimal.
- Add or update tests for any behavior change.
- Update documentation when the user-visible workflow changes.
- Preserve existing CLI commands and fixture formats unless a change is intentional.

## Submitting Changes

1. Create a branch for your work.
2. Make the smallest change that solves the problem.
3. Run the relevant tests before opening a pull request.
4. Describe what changed, how it was tested, and any follow-up work.

If your change affects the unified metadata/publications pipeline, include the fixture or publication test case that proves it.