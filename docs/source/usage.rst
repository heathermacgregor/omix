Usage
=====

This page gives a practical overview of the main CLI workflows.

Fetch Metadata
--------------

.. code-block:: bash

   omix fetch-metadata samples.tsv --email you@example.com

This command reads a table of sample or study accessions and enriches it with
ENA metadata such as run accessions, sample accessions, geography, and
sequencing fields.

Fetch Publications
------------------

.. code-block:: bash

   omix fetch-publications PRJNA864623 --omics 16S --api-key $LLM_KEY

Use this when you want publication discovery and methodology extraction for a
specific accession.

Unified Pipeline
----------------

.. code-block:: bash

   omix enrich-with-publications samples.csv -o enriched.csv --config config.debug.yaml

The unified pipeline performs four steps:

1. Metadata enrichment
2. Publication discovery
3. Publication validation
4. Integration of publication fields into the metadata table

Useful flags:

* ``--no-validate`` keeps raw publication hits.
* ``--no-llm`` disables LLM-based methodology extraction.
* ``--builtin`` uses the built-in primer database.
* ``--primer-db`` points at a custom probeBase database.

Example Development Run
-----------------------

.. code-block:: bash

   omix enrich-with-publications tests/fixtures/amplicon_20/demo_input_3_studies.csv \
     -o /tmp/enriched.csv \
     --config config.debug.yaml \
     --email test@example.com
