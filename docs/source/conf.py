"""Sphinx configuration for the omix documentation site."""

from __future__ import annotations

import sys
from pathlib import Path

# Adjust ROOT to go up two levels (docs/source/conf.py -> project root)
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

project = "omix"
author = "Heather MacGregor"
copyright = "2026, Heather MacGregor"

with open(ROOT / "omix" / "__init__.py", encoding="utf-8") as handle:
    for line in handle:
        if line.startswith("__version__"):
            version = release = line.split("=")[-1].strip().strip('"\'')
            break
    else:
        version = release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}
autodoc_typehints = "description"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_title = "omix documentation"
html_show_sourcelink = True
