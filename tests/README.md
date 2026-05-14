# Tests Layout

This directory is reserved for automated test assets and test data.

## Structure

- `unit/`: Fast unit tests.
- `integration/`: Integration tests that may touch external services or larger fixtures.
- `fixtures/`: Reusable CSV, JSON, database, and log fixtures used by tests and demos.
- `manual/`: Ad hoc scripts that exercise the pipeline or validate generated outputs.
- `artifacts/`: Generated outputs kept for inspection and regression comparison.

Root-level pytest wiring lives in `conftest.py` and package import support in `__init__.py`.