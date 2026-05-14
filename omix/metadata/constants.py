"""
Default mappings, ontological data, and ENA field definitions for metadata normalization.

These constants are used by MetadataManager and MetadataEnricher to standardize
heterogeneous metadata from different sources.
"""

import re
from typing import Dict, List, Tuple, Callable

import pandas as pd

# ---- Numeric and pH patterns ----
NUM_PATTERN = re.compile(r'[-+]?\d*\.\d+|[-+]?\d+')
PH_PATTERN = re.compile(r'^ph[^a-zA-Z]|^ph$')

# ---- Coordinate detection: explicit lat/lon columns and pair columns ----
DEFAULT_COORDINATE_SOURCES = {
    'lat': [
        'lat_study', 'lat_ena', 'lat.1', 'lat',
        'biosample_geographic_location_(latitude)',
        'biosample_latitude', 'experiment_lat', 'run_lat', 'latitude'
    ],
    'lon': [
        'lon_study', 'lon.1', 'lon',
        'biosample_geographic_location_(longitude)',
        'biosample_longitude', 'experiment_lon', 'run_lon', 'longitude'
    ],
    'pairs': [
        'location_ena', 'location_start', 'location_end', 'location_start_study',
        'location_end_study', 'lat_lon', 'location', 'biosample_lat_lon',
        'biosample_latitude_and_longitude', 'run_location', 'run_location_start',
        'run_location_end', 'experiment_location', 'experiment_location_start',
        'experiment_location_end'
    ]
}

# ---- Standard column name mappings ----
DEFAULT_COLUMN_MAPPINGS = {
    'env_biome': 'environment_biome',
    'env_feature': 'environment_feature',
    'env_material': 'environment_material'
}

# ---- Unit detection patterns ----
DEFAULT_UNIT_PATTERNS = {
    'celsius': re.compile(r'_(?:celsius|cel|c)$', re.IGNORECASE),
    'fahrenheit': re.compile(r'_(?:fahrenheit|far|f)$', re.IGNORECASE),
    'kelvin': re.compile(r'_(?:kelvin|k)$', re.IGNORECASE),
    'meters': re.compile(r'_(?:meters|meter|m)$', re.IGNORECASE),
    'feet': re.compile(r'_(?:feet|ft)$', re.IGNORECASE)
}

# ---- Unit conversion functions ----
DEFAULT_CONVERSIONS: Dict[str, Tuple[str, Callable[[pd.Series], pd.Series]]] = {
    'fahrenheit': ('celsius', lambda f: (pd.to_numeric(f, errors='coerce') - 32) * 5 / 9),
    'kelvin': ('celsius', lambda k: pd.to_numeric(k, errors='coerce') - 273.15),
    'feet': ('meters', lambda ft: pd.to_numeric(ft, errors='coerce') * 0.3048),
}

# ---- Preferred units for physical quantities ----
DEFAULT_MEASUREMENT_STANDARDS = {
    'temp': 'celsius',
    'depth': 'meters',
    'altitude': 'meters'
}

# ---- Ontology inference maps (keyword → category) ----
ONTOLOGY_MAP = {
    'empo_1': {
        'Host-associated': [
            'host', 'symbiont', 'microbiome', 'human', 'animal'
        ],
        'Free-living': [
            'free living', 'environmental', 'soil', 'water', 'sediment', 'air'
        ]
    },
    'empo_2': {
        'Animal': ['animal', 'human', 'insect', 'mammal', 'gut', 'feces', 'skin'],
        'Plant': ['plant', 'rhizosphere', 'root', 'leaf', 'flower'],
        'Fungus': ['fungus', 'fungal'],
        'Aquatic': ['aquatic', 'water', 'marine', 'freshwater', 'sediment', 'ocean'],
        'Terrestrial': ['terrestrial', 'soil', 'land', 'desert', 'forest']
    },
    'empo_3': {
        'Gut': ['gut', 'feces', 'fecal', 'intestinal'],
        'Soil': ['soil', 'rhizosphere', 'terrestrial'],
        'Water': ['water', 'aquatic', 'marine', 'freshwater'],
        'Sediment': ['sediment'],
        'Skin': ['skin']
    },
    'env_biome': {
        'Urban': ['urban', 'city'],
        'Agricultural': ['agricultural', 'farm', 'crop'],
        'Forest': ['forest'],
        'Grassland': ['grassland', 'savanna'],
        'Aquatic': ['aquatic', 'marine', 'freshwater', 'lake', 'river', 'ocean']
    },
    'env_feature': {
        'Anthropogenic': ['anthropogenic', 'human-made', 'built environment'],
        'Natural': ['natural', 'wild']
    },
    'env_material': {
        'Soil': ['soil', 'loam', 'clay', 'silt'],
        'Water': ['water'],
        'Sediment': ['sediment', 'mud'],
        'Air': ['air']
    }
}

# ---- Keywords used to classify host-associated samples ----
exclusion_keywords = [
    "human", "patient", "clinical",
    "mouse", "mice", "murine",
    "rat", "rattus",
    "bovine", "cattle", "cow",
    "porcine", "pig", "swine",
    "avian", "chicken", "poultry",
    "ovine", "sheep",
    "canine", "dog",
    "feline", "cat",
    "equine", "horse",
    "primate", "monkey", "ape",
    "animal model", "host", "host-associated",
    "gut", "gastrointestinal", "intestinal",
    "oral", "mouth", "dental", "plaque",
    "organ", "tissue", "biopsy",
    "skin", "dermal", "cutaneous",
    "lung", "pulmonary", "respiratory",
    "vaginal", "urogenital",
    "nasal", "nasopharyngeal",
    "brain", "neural",
    "liver", "hepatic",
    "kidney", "renal",
    "feces", "fecal", "stool", "scat",
    "blood", "serum", "plasma",
    "saliva", "sputum",
    "urine", "urinary",
    "milk", "mammary",
    "mucus", "mucosal",
    "semen", "seminal",
    "bile",
    "disease", "disorder", "syndrome",
    "infection", "infectious", "pathogen",
    "immune", "immunity", "immunological",
    "inflammation", "inflammatory",
    "lesion", "wound", "abscess",
    "cancer", "tumor", "carcinoma",
    "health", "healthy", "control",
    "treatment", "therapy", "antibiotic",
    "probiotic", "prebiotic",
    "vaginal microbiome", "microbiota", "dysbiosis",
    "gut microbiome", "oral microbiome",
    "holobiont"
]

# ---- ENA API endpoints (used by the ena subpackage) ----
ENA_API_URL = "https://www.ebi.ac.uk/ena/portal/api/search"
BIOSAMPLES_API_URL = "https://www.ebi.ac.uk/biosamples/samples/"