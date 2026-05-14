"""
ENA/SRA Sample Accession Parsing and Project Resolution.

Handles:
- Parsing various sample ID formats (ENA/SRA accessions, raw identifiers)
- Resolving parent study/project information
- Building sample-to-project mappings
- Implementing rate limiting and caching

Supported accession formats:
- ENA: SAMEA (primary), SAMN (secondary)
- SRA: SRP (study), SRX (experiment), SRR (run), SRS (sample)
- Direct run accessions (SRR)
- Compound: RUN_ACCESSION.SAMPLE_ACCESSION (e.g., SRR10009574.SRS5298044)
"""

import asyncio
import re
import time
from dataclasses import dataclass, field, asdict
from hashlib import sha256
from typing import Dict, List, Optional, Set, Tuple, Any

import aiohttp

from omix.logging_utils import get_logger
from .cache import SQLiteCacheManager
from ..constants import ENA_API_URL

logger = get_logger("omix.ena.sample_parser")


# ===================== DATA CLASSES ===================== #

@dataclass
class ParsedSample:
    """Represents a parsed sample ID with normalized accession(s)."""

    raw_id: str
    """Original raw input ID."""

    accession_type: str
    """Type of accession: SAMEA, SAMN, SRP, SRX, SRR, SRS, or UNKNOWN."""

    primary_accession: Optional[str] = None
    """Primary ENA accession (SAMEA)."""

    secondary_accession: Optional[str] = None
    """Secondary ENA accession (SAMN)."""

    sra_accessions: Dict[str, str] = field(default_factory=dict)
    """SRA accessions indexed by type: {SRP: '...', SRX: '...', SRR: '...', SRS: '...'}"""

    is_valid: bool = False
    """Whether this sample was successfully parsed."""

    confidence: float = 0.0
    """Confidence score (0.0-1.0) for parsing accuracy."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        return result

    def get_all_accessions(self) -> Set[str]:
        """Get all known accessions for this sample."""
        accessions = set()
        if self.primary_accession:
            accessions.add(self.primary_accession)
        if self.secondary_accession:
            accessions.add(self.secondary_accession)
        accessions.update(self.sra_accessions.values())
        return accessions


@dataclass
class ProjectInfo:
    """Information about a study/project."""

    study_accession: str
    """Study/project accession (e.g., SRP, PRJNA)."""

    study_title: Optional[str] = None
    """Study title."""

    study_abstract: Optional[str] = None
    """Study abstract/description."""

    center_name: Optional[str] = None
    """Sequencing center name."""

    sample_count: Optional[int] = None
    """Number of samples in this study."""

    first_public: Optional[str] = None
    """Date first published."""

    last_update: Optional[str] = None
    """Date last updated."""

    additional_fields: Dict[str, Any] = field(default_factory=dict)
    """Additional metadata fields from ENA."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        return result


# ===================== ACCESSION VALIDATION ===================== #

class AccessionValidator:
    """Validates and classifies ENA/SRA accessions."""

    # Regex patterns for different accession types
    PATTERNS = {
        'SAMEA': re.compile(r'^SAMEA\d{7,}$'),          # ENA primary
        'SAMN':  re.compile(r'^SAMN\d{7,}$'),           # NCBI secondary
        'SRP':   re.compile(r'^SRP\d{6,}$'),            # SRA study
        'SRX':   re.compile(r'^SRX\d{6,}$'),            # SRA experiment
        'SRR':   re.compile(r'^SRR\d{6,}$'),            # SRA run
        'SRS':   re.compile(r'^SRS\d{6,}$'),            # SRA sample
        'PRJNA': re.compile(r'^PRJNA\d+$'),             # NCBI BioProject
        'PRJEB': re.compile(r'^PRJEB\d+$'),             # ENA project
        'ERS':   re.compile(r'^ERS\d{6,}$'),            # ENA sample (secondary) - 6+ digits
        'DRR':   re.compile(r'^DRR\d{6,}$'),            # DDBJ run
        'DRS':   re.compile(r'^DRS\d{6,}$'),            # DDBJ sample
        'ERR':   re.compile(r'^ERR\d{6,}$'),            # ENA run
        'COMPOUND': re.compile(
            r'^(SRR|ERR|DRR|SRX)\d{6,}\.(SRS|ERS|DRS|SAMEA|SAMN)\d{6,}$'
        ),  # Compound format: RUN.SAMPLE
    }

    @classmethod
    def validate_accession(cls, accession: str) -> Optional[str]:
        """
        Validate an accession and return its type, or None if invalid.

        Args:
            accession: The accession string to validate.

        Returns:
            The accession type (e.g., 'SAMEA', 'SRR') or None if invalid.
        """
        accession = accession.strip().upper()

        for acc_type, pattern in cls.PATTERNS.items():
            if pattern.match(accession):
                return acc_type

        return None

    @classmethod
    def classify_accession(cls, accession: str) -> Tuple[Optional[str], float]:
        """
        Classify an accession with confidence score.

        Args:
            accession: The accession string to classify.

        Returns:
            Tuple of (accession_type, confidence_score).
        """
        accession_type = cls.validate_accession(accession)

        if accession_type:
            return accession_type, 1.0

        # Try fuzzy matching for partial accessions
        accession = accession.strip().upper()

        if accession.startswith('SAMEA'):
            return 'SAMEA', 0.7
        elif accession.startswith('SAMN'):
            return 'SAMN', 0.7
        elif accession.startswith('SRP'):
            return 'SRP', 0.7
        elif accession.startswith('SRX'):
            return 'SRX', 0.7
        elif accession.startswith('SRR'):
            return 'SRR', 0.7
        elif accession.startswith('SRS'):
            return 'SRS', 0.7
        elif accession.startswith('ERS'):
            return 'ERS', 0.7
        elif accession.startswith('ERR'):
            return 'ERR', 0.7
        elif accession.startswith('DRS'):
            return 'DRS', 0.7
        elif accession.startswith('DRR'):
            return 'DRR', 0.7

        return None, 0.0


# ===================== SAMPLE PARSER ===================== #

class SampleParser:
    """
    Parses sample IDs and resolves project information.

    Supports:
    - ENA accessions (SAMEA, SAMN, ERS)
    - SRA accessions (SRP, SRX, SRR, SRS)
    - Raw sample identifiers (with fuzzy matching)
    """

    def __init__(
        self,
        cache_manager: Optional[SQLiteCacheManager] = None,
        max_concurrent: int = 10,
    ):
        """
        Initialize the SampleParser.

        Args:
            cache_manager: Cache manager for storing query results.
            max_concurrent: Maximum concurrent API requests.
        """
        self.cache_manager = cache_manager
        self.max_concurrent = max_concurrent
        self.session: Optional[aiohttp.ClientSession] = None
        self.validator = AccessionValidator()

        # Rate limiting
        self.min_request_interval = 1.0 / 5.0  # 5 requests per second (respects ENA limit)
        self.last_request_time = 0.0
        # Lock will be created on first async call (event loop exists then)
        self.request_lock: Optional[asyncio.Lock] = None

        # Backoff configuration
        self.max_retries = 3
        self.initial_backoff = 2

    async def __aenter__(self):
        """Async context manager entry."""
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            use_dns_cache=True,
        )
        timeout = aiohttp.ClientTimeout(total=150, connect=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "omix SampleParser/1.0"},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    def _get_cache_key(self, prefix: str, *args) -> str:
        """Generate cache key from prefix and arguments."""
        representation = f"{prefix}:" + ":".join(map(str, args))
        return sha256(representation.encode()).hexdigest()

    @staticmethod
    def _validate_email(email: str) -> bool:
        """
        Validate email format using regex.

        Args:
            email: Email string to validate

        Returns:
            True if valid email format, False otherwise
        """
        email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        return bool(email_pattern.match(email))

    async def _initialize_lock(self) -> None:
        """Initialize lock on first async operation (event loop now exists)."""
        if self.request_lock is None:
            self.request_lock = asyncio.Lock()

    async def _rate_limited_request(self):
        """Enforce rate limiting before API requests."""
        await self._initialize_lock()
        async with self.request_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_request_interval:
                await asyncio.sleep(self.min_request_interval - elapsed)
            self.last_request_time = time.time()

    def parse_sample_ids(
        self,
        sample_ids: List[str]
    ) -> Dict[str, ParsedSample]:
        """
        Parse sample IDs synchronously.

        Args:
            sample_ids: List of sample IDs to parse.

        Returns:
            Dictionary mapping raw ID to ParsedSample.
        """
        return asyncio.run(self.parse_sample_ids_async(sample_ids))

    async def parse_sample_ids_async(
        self,
        sample_ids: List[str]
    ) -> Dict[str, ParsedSample]:
        """
        Parse sample IDs asynchronously.

        Args:
            sample_ids: List of sample IDs to parse.

        Returns:
            Dictionary mapping raw ID to ParsedSample.
        """
        results = {}
        for sample_id in sample_ids:
            parsed = await self._parse_single_sample_id(sample_id)
            results[sample_id] = parsed
            logger.debug(
                f"Parsed {sample_id} -> {parsed.accession_type} "
                f"(confidence: {parsed.confidence:.2f})"
            )
        return results

    async def _parse_single_sample_id(self, sample_id: str) -> ParsedSample:
        """Parse a single sample ID."""
        sample_id = sample_id.strip()

        # Try standard validation first
        acc_type, confidence = self.validator.classify_accession(sample_id)

        # Handle compound format (RUN.SAMPLE) - check directly if matched
        if acc_type == 'COMPOUND' and confidence >= 1.0:
            parts = sample_id.split('.')
            if len(parts) == 2:
                run_id, sample_accession = parts
                run_type, run_conf = self.validator.classify_accession(run_id)
                sample_type, sample_conf = self.validator.classify_accession(sample_accession)

                # Valid compound if both parts are valid run and sample accessions
                if (
                    run_type in ['SRR', 'ERR', 'DRR', 'SRX'] and run_conf >= 1.0
                    and sample_type in ['SRS', 'ERS', 'DRS', 'SAMEA', 'SAMN']
                    and sample_conf >= 1.0
                ):
                    return ParsedSample(
                        raw_id=sample_id,
                        accession_type='COMPOUND',
                        secondary_accession=run_id,  # Run accession
                        sra_accessions={
                            run_type: run_id,
                            sample_type: sample_accession,
                        },
                        is_valid=True,
                        confidence=1.0,
                    )

        # Check for compound format fallback (in case pattern doesn't match directly)
        if '.' in sample_id and acc_type != 'COMPOUND' and confidence < 1.0:
            parts = sample_id.split('.')
            if len(parts) == 2:
                run_id, sample_accession = parts
                run_type, run_conf = self.validator.classify_accession(run_id)
                sample_type, sample_conf = self.validator.classify_accession(sample_accession)

                if (
                    run_type in ['SRR', 'ERR', 'DRR', 'SRX'] and run_conf >= 1.0
                    and sample_type in ['SRS', 'ERS', 'DRS', 'SAMEA', 'SAMN']
                    and sample_conf >= 1.0
                ):
                    return ParsedSample(
                        raw_id=sample_id,
                        accession_type='COMPOUND',
                        secondary_accession=run_id,
                        sra_accessions={
                            run_type: run_id,
                            sample_type: sample_accession,
                        },
                        is_valid=True,
                        confidence=1.0,
                    )

        if acc_type and confidence >= 1.0:
            # Valid accession (but not COMPOUND)
            if acc_type != 'COMPOUND':
                return ParsedSample(
                    raw_id=sample_id,
                    accession_type=acc_type,
                    primary_accession=sample_id if acc_type == 'SAMEA' else None,
                    secondary_accession=(
                        sample_id if acc_type in ['SAMN', 'ERS'] else None
                    ),
                    sra_accessions=(
                        {acc_type: sample_id}
                        if acc_type in ['SRP', 'SRX', 'SRR', 'SRS', 'ERR', 'DRR', 'DRS']
                        else {}
                    ),
                    is_valid=True,
                    confidence=confidence,
                )
        elif acc_type and confidence >= 0.7:
            # Partial match (fuzzy)
            return ParsedSample(
                raw_id=sample_id,
                accession_type=acc_type,
                is_valid=True,
                confidence=confidence,
            )
        else:
            # Unknown format – store as raw identifier
            return ParsedSample(
                raw_id=sample_id,
                accession_type='UNKNOWN',
                is_valid=False,
                confidence=0.0,
            )

    async def resolve_projects(
        self,
        sample_accessions: List[str],
        email: str,
    ) -> Dict[str, ProjectInfo]:
        """
        Resolve project/study information for samples.

        Args:
            sample_accessions: List of sample accessions.
            email: Email for ENA API requests.

        Returns:
            Dictionary mapping sample accession to ProjectInfo.
        """
        if not self._validate_email(email):
            raise ValueError(f"Invalid email format: {email}")

        if not sample_accessions:
            return {}

        # Use dict.fromkeys() to preserve order
        unique_accessions = list(dict.fromkeys(sample_accessions))
        results = {}

        # Check cache first
        cached = {}
        if self.cache_manager:
            cache_results = await self.cache_manager.get_bulk(
                [self._get_cache_key("project", acc) for acc in unique_accessions]
            )
            for acc in unique_accessions:
                cache_key = self._get_cache_key("project", acc)
                if cache_key in cache_results:
                    cached[acc] = cache_results[cache_key]

        to_fetch = [acc for acc in unique_accessions if acc not in cached]

        if cached:
            logger.info(f"Found {len(cached)} projects in cache")
            for acc, project_data in cached.items():
                results[acc] = ProjectInfo(**project_data)

        if to_fetch:
            logger.info(f"Resolving {len(to_fetch)} new projects from ENA...")
            fetched = await self._fetch_projects_for_accessions(to_fetch, email)

            if self.cache_manager and fetched:
                for acc, project_info in fetched.items():
                    cache_key = self._get_cache_key("project", acc)
                    await self.cache_manager.set(cache_key, project_info.to_dict())

            results.update(fetched)

        return results

    async def _fetch_projects_for_accessions(
        self,
        accessions: List[str],
        email: str,
    ) -> Dict[str, ProjectInfo]:
        """Fetch project info for accessions from ENA."""
        results = {}

        chunk_size = 50
        chunks = [accessions[i:i + chunk_size] for i in range(0, len(accessions), chunk_size)]

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_chunk(chunk):
            async with semaphore:
                return await self._fetch_project_chunk(chunk, email)

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in chunk_results:
            if isinstance(result, dict):
                results.update(result)
            elif isinstance(result, Exception):
                logger.error(f"Error fetching project chunk: {result}")

        return results

    async def _fetch_project_chunk(
        self,
        accessions: List[str],
        email: str,
    ) -> Dict[str, ProjectInfo]:
        """Fetch project info for a chunk of accessions."""
        results = {}
        for accession in accessions:
            project_info = await self._fetch_single_project(accession, email)
            if project_info:
                results[accession] = project_info
        return results

    async def _fetch_single_project(
        self,
        accession: str,
        email: str,
    ) -> Optional[ProjectInfo]:
        """Fetch project info for a single accession with retries."""
        if not self._validate_email(email):
            raise ValueError(f"Invalid email format: {email}")

        acc_type = (
            self.validator.validate_accession(accession)
            or AccessionValidator.classify_accession(accession)[0]
        )
        if not acc_type:
            logger.warning(f"Cannot determine type for accession: {accession}")
            return None

        query_key_map = {
            'SAMEA': 'sample_accession',
            'SAMN': 'secondary_sample_accession',
            'SRR': 'run_accession',
            'SRS': 'sample_accession',
            'SRX': 'experiment_accession',
            'SRP': 'study_accession',
            'ERS': 'sample_accession',
        }
        query_key = query_key_map.get(acc_type)
        if not query_key:
            logger.warning(f"No query mapping for accession type: {acc_type}")
            return None

        for attempt in range(self.max_retries):
            try:
                await self._rate_limited_request()

                if not self.session or self.session.closed:
                    raise ConnectionError(
                        f"Session closed while fetching {accession}."
                    )

                result_type = self._get_result_type_for_query_key(query_key)
                params = {
                    "result": result_type,
                    "query": f'{query_key}="{accession}"',
                    "fields": "all",
                    "format": "json",
                    "limit": 1,
                }

                async with self.session.get(ENA_API_URL, params=params) as response:
                    if response.status == 204:
                        return None
                    if response.status != 200:
                        continue

                    data = await response.json()
                    if not data:
                        return None

                    item = data[0] if isinstance(data, list) else data
                    study_accession = item.get('study_accession') or item.get('project')
                    if not study_accession:
                        logger.debug(f"No study_accession found for {accession}")
                        return None

                    return await self._fetch_study_details(study_accession, email)

            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    backoff = self.initial_backoff * (2 ** attempt)
                    await asyncio.sleep(backoff)
            except ConnectionError as e:
                logger.error(f"Connection error fetching {accession}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.initial_backoff * (2 ** attempt))
            except Exception as e:
                logger.error(f"Error fetching {accession}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.initial_backoff * (2 ** attempt))

        return None

    async def _fetch_study_details(
        self,
        study_accession: str,
        email: str,
    ) -> Optional[ProjectInfo]:
        """Fetch study/project details from ENA."""
        try:
            await self._rate_limited_request()

            if not self.session or self.session.closed:
                raise ConnectionError(
                    f"Session closed while fetching study {study_accession}."
                )

            params = {
                "result": "study",
                "query": f'study_accession="{study_accession}"',
                "fields": "all",
                "format": "json",
                "limit": 1,
            }

            async with self.session.get(ENA_API_URL, params=params) as response:
                if response.status in (204, 404):
                    return None
                if response.status != 200:
                    return None

                data = await response.json()
                if not data:
                    return None

                item = data[0] if isinstance(data, list) else data

                return ProjectInfo(
                    study_accession=item.get('study_accession', study_accession),
                    study_title=item.get('study_title'),
                    study_abstract=item.get('study_abstract'),
                    center_name=item.get('center_name'),
                    first_public=item.get('first_public'),
                    last_update=item.get('last_update'),
                    additional_fields={
                        k: v
                        for k, v in item.items()
                        if k not in [
                            'study_accession', 'study_title', 'study_abstract',
                            'center_name', 'first_public', 'last_update',
                        ]
                    },
                )
        except Exception as e:
            logger.error(f"Error fetching study details for {study_accession}: {e}")
            return None

    def _get_result_type_for_query_key(self, query_key: str) -> str:
        """Map query key to ENA result type."""
        mapping = {
            'sample_accession': 'sample',
            'secondary_sample_accession': 'sample',
            'run_accession': 'read_run',
            'experiment_accession': 'experiment',
            'study_accession': 'study',
        }
        return mapping.get(query_key, 'sample')

    def group_samples_by_project(
        self,
        sample_to_project: Dict[str, ProjectInfo],
    ) -> Dict[str, List[str]]:
        """
        Group samples by their project.

        Args:
            sample_to_project: Dictionary mapping sample accession to ProjectInfo.

        Returns:
            Dictionary mapping project_accession to list of sample accessions.
        """
        project_to_samples: Dict[str, List[str]] = {}
        for sample_accession, project_info in sample_to_project.items():
            project_acc = project_info.study_accession
            if project_acc not in project_to_samples:
                project_to_samples[project_acc] = []
            project_to_samples[project_acc].append(sample_accession)
        return project_to_samples


# ===================== CONVENIENCE FUNCTIONS ===================== #

async def parse_sample_ids(
    sample_ids: List[str],
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> Dict[str, ParsedSample]:
    """
    Parse sample IDs asynchronously.

    Args:
        sample_ids: List of sample IDs to parse.
        cache_manager: Optional cache manager for storing results.

    Returns:
        Dictionary mapping raw ID to ParsedSample.
    """
    async with SampleParser(cache_manager=cache_manager) as parser:
        return await parser.parse_sample_ids_async(sample_ids)


async def resolve_projects(
    sample_accessions: List[str],
    email: str,
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> Dict[str, ProjectInfo]:
    """
    Resolve project/study information for samples.

    Args:
        sample_accessions: List of sample accessions.
        email: Email for ENA API requests.
        cache_manager: Optional cache manager for storing results.

    Returns:
        Dictionary mapping sample accession to ProjectInfo.
    """
    async with SampleParser(cache_manager=cache_manager) as parser:
        return await parser.resolve_projects(sample_accessions, email)


def parse_sample_ids_sync(
    sample_ids: List[str],
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> Dict[str, ParsedSample]:
    """
    Parse sample IDs synchronously.

    Note: This function should only be called from non-async contexts.
    For async code, use parse_sample_ids() or SampleParser directly.

    Args:
        sample_ids: List of sample IDs to parse.
        cache_manager: Optional cache manager for storing results.

    Returns:
        Dictionary mapping raw ID to ParsedSample.
    """
    try:
        asyncio.get_running_loop()
        raise RuntimeError(
            "parse_sample_ids_sync() cannot be called from an async context. "
            "Use SampleParser.parse_sample_ids_async() or parse_sample_ids() instead."
        )
    except RuntimeError as e:
        if "parse_sample_ids_sync()" in str(e):
            raise
        return asyncio.run(parse_sample_ids(sample_ids, cache_manager))


def resolve_projects_sync(
    sample_accessions: List[str],
    email: str,
    cache_manager: Optional[SQLiteCacheManager] = None,
) -> Dict[str, ProjectInfo]:
    """
    Resolve project/study information for samples synchronously.

    Note: This function should only be called from non-async contexts.
    For async code, use resolve_projects() or SampleParser directly.

    Args:
        sample_accessions: List of sample accessions.
        email: Email for ENA API requests.
        cache_manager: Optional cache manager for storing results.

    Returns:
        Dictionary mapping sample accession to ProjectInfo.
    """
    try:
        asyncio.get_running_loop()
        raise RuntimeError(
            "resolve_projects_sync() cannot be called from an async context. "
            "Use SampleParser.resolve_projects() or resolve_projects() instead."
        )
    except RuntimeError as e:
        if "resolve_projects_sync()" in str(e):
            raise
        return asyncio.run(resolve_projects(sample_accessions, email, cache_manager))