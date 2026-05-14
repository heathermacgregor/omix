"""
Base class for publication API wrappers.

Provides:
- Robust HTTP session with retries.
- Rate limiting helpers.
- Standardised search method signature (implements `PublicationSource`).
"""

import time
import random
from functools import wraps
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from omix.publications.base import PublicationSource
from omix.publications.exceptions import InvalidAPIKeyError
from omix.logging_utils import get_logger


# --------------------------------------------------------------------------- #
#  Retry decorator
# --------------------------------------------------------------------------- #

def with_http_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 32.0,
):
    """
    FEATURE 3: Decorator that handles HTTP 429 / 5xx errors with exponential backoff.
    Configurable per API source.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                        if retries >= max_retries:
                            raise
                        sleep_time = min(max_delay, base_delay * (2 ** retries))
                        jitter = random.uniform(0, 0.1 * sleep_time)
                        total_sleep = sleep_time + jitter
                        logger = get_logger("omix.apis.base")
                        logger.warning(
                            f"Rate limit / server error (HTTP {e.response.status_code}). "
                            f"Retrying in {total_sleep:.1f}s "
                            f"(attempt {retries + 1}/{max_retries})"
                        )
                        time.sleep(total_sleep)
                        retries += 1
                    else:
                        raise
        return wrapper
    return decorator


# --------------------------------------------------------------------------- #
#  Base class
# --------------------------------------------------------------------------- #

class BasePublicationAPI(PublicationSource):
    """
    Common base for all publication API wrappers.

    Subclasses must:
    - Set `base_url` and `source_name`.
    - Implement `search()`.

    Provides:
    - `self.session` – a robust, pooled `requests.Session`.
    - `self._rate_limit(api_name)` – simple fixed‑window rate limiter.
    - `self.email` – for polite API identification.
    - FEATURE 3: Configurable retry/backoff per API source.
    """

    def __init__(
        self,
        email: str,
        timeout: int = 30,
        # FEATURE 3: Retry/backoff config (optional)
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 32.0,
    ):
        self.email = email
        self.timeout = timeout
        self.base_url = ""                 # override in subclass
        self._source_name = "base"
        
        # FEATURE 3: Store retry config for use in decorators
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

        # Rate limiting state
        self._last_request_times: Dict[str, float] = {}
        self._rate_limits: Dict[str, float] = {'default': 0.5}

        # Robust session
        self.session = self._build_session()

    @property
    def source_name(self) -> str:
        return self._source_name

    # ------------------------------------------------------------------
    #  Session builder
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": f"omix PublicationAPI/1.0 (mailto:{self.email})",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # ------------------------------------------------------------------
    #  Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self, api_name: str = 'default') -> None:
        """Sleep if necessary to respect the per‑API rate limit."""
        current = time.time()
        last = self._last_request_times.get(api_name, 0)
        min_interval = self._rate_limits.get(api_name, self._rate_limits['default'])
        elapsed = current - last
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_times[api_name] = time.time()

    # ------------------------------------------------------------------
    #  Abstract search (to be overridden)
    # ------------------------------------------------------------------

    def search(self, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement search()")
    
    def check_key_error(self, response: requests.Response, api_name: str) -> None:
        """Raise InvalidAPIKeyError if the response indicates a bad key."""
        if response.status_code in (401, 403):
            raise InvalidAPIKeyError(
                api_name,
                "The API key is invalid or lacks required permissions. "
                "Please update your config file or set the corresponding environment variable."
            )