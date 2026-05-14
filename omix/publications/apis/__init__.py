"""
Publication API wrappers.

Each module implements the `PublicationSource` interface for a specific
publication database or search engine.
"""

from .base import BasePublicationAPI
from .crossref import CrossrefAPI
from .europe_pmc import EuropePMCAPI
from .ncbi import NCBIAPI
from .semantic_scholar import SemanticScholarAPI
from .arxiv import ArxivAPI
from .basesearch import BaseSearchAPI
from .bioarxiv import BioarxivAPI
from .core import CoreAPI
from .datacite import DataciteAPI
from .doaj import DOAJAPI
from .lens import LensAPI
from .mendeley import MendeleyAPI
from .plos import PLOSAPI
from .springer_nature import SpringerNatureAPI
from .unpaywall import UnpaywallAPI
from .zenodo import ZenodoAPI