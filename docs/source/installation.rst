Installation
============

`omix` targets Python 3.10 and newer.

Stable Install
--------------

Install from PyPI:

.. code-block:: bash

   pip install omix

Optional features are available through extras:

.. code-block:: bash

   pip install omix[llm]

Development Setup
-----------------

For local development in this repository, use an editable install:

.. code-block:: bash

   pip install -e .[dev,docs]

The `dev` extra installs the test dependencies, and the `docs` extra installs
Sphinx plus the Read the Docs theme.

Local Environment
-----------------

The repository includes a local virtual environment at ``omix_env/``. You can
activate it before installing or running the project:

.. code-block:: bash

   source omix_env/bin/activate

If you prefer a fresh environment, create one with ``python -m venv .venv`` and
install the package from the repository root.
