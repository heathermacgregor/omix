Configuration
=============

`omix` reads configuration from YAML files and environment variables. The most
important settings are shown below.

Credentials
-----------

.. code-block:: yaml

   credentials:
     email: "your.email@example.com"
     ena_email: "ena@example.com"
     llm_api_key: "sk-..."
     ncbi_api_key: "..."

The ``email`` value is used for polite API requests. The ENA email can be set
separately if you want a dedicated contact address for metadata calls.

API Settings
------------

.. code-block:: yaml

   apis:
     enabled: true
     sequence:
       ena:
         enabled: true
         max_concurrent: 5
         batch_size: 100
         cache_ttl_days: 30
         fetch_phases: true

These controls affect metadata discovery and rate-limited API access.

Metadata Settings
-----------------

.. code-block:: yaml

   metadata:
     sample_id_column: "#sampleid"
     exclude_host: false
     columns_to_drop: []
     force_numeric_columns:
       - lat
       - lon

The sample ID column is especially important when importing CSV files that use
an alternate header like ``#sampleid`` or ``sampleid``.

Publication Settings
--------------------

.. code-block:: yaml

   publication:
     max_retries: 3
     base_delay_seconds: 1.0
     max_delay_seconds: 30.0
     request_timeout_seconds: 30

Lower delays are useful when running local tests with ``config.debug.yaml``.

Paths
-----

.. code-block:: yaml

   paths:
     cache_dir: ".cache"
     logs_dir: "logs"
     primer_db: null

These paths control cache storage, log output, and an optional custom primer
database.

Debug Configuration
-------------------

The repository includes ``config.debug.yaml`` for fast local testing. Use it
when you want lower timeouts and deterministic credentials for the test suite.
