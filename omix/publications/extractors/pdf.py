"""
PDF parsing utilities for publication full‑text extraction.

Uses pdfplumber (optional) to extract text from PDF files while enforcing
page and file‑size limits.
"""

import io
from typing import Optional

import requests

from omix.logging_utils import get_logger

logger = get_logger("omix.extractors.pdf")

# Optional dependency
try:
    import pdfplumber  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    pdfplumber = None

# --------------------------------------------------------------------------- #
#  Configuration
# --------------------------------------------------------------------------- #

MAX_FILE_SIZE = 50 * 1024 * 1024   # 50 MB
MAX_PDF_PAGES = 10                 # Limit extracted pages


# --------------------------------------------------------------------------- #
#  Public API
# --------------------------------------------------------------------------- #

def fetch_and_parse_pdf(url: str, session: requests.Session) -> Optional[str]:
    """
    Download a PDF from `url` and return its extracted text.

    Args:
        url: Direct URL to a PDF file.
        session: A requests.Session object (used for connection pooling).

    Returns:
        Extracted text as a single string, or None if the PDF could not be
        retrieved or parsed.
    """
    if pdfplumber is None:
        logger.debug("pdfplumber not installed; skipping PDF text extraction.")
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf",
    }

    try:
        response = session.get(url, headers=headers, timeout=30, stream=True)
    except requests.RequestException:
        logger.debug(f"Failed to fetch {url}")
        return None

    if response.status_code != 200:
        logger.debug(f"HTTP {response.status_code} for {url}")
        return None

    # Verify content type
    content_type = response.headers.get('Content-Type', '').lower()
    if 'application/pdf' not in content_type:
        logger.debug(f"URL {url} returned {content_type} instead of PDF")
        return None

    # Check file size
    content_length = response.headers.get('Content-Length')
    if content_length and int(content_length) > MAX_FILE_SIZE:
        logger.debug(f"PDF too large ({content_length} bytes) – skipping")
        return None

    # Read content
    try:
        pdf_content = response.content
    except requests.RequestException:
        logger.debug(f"Failed to read PDF content from {url}")
        return None

    if not pdf_content.startswith(b"%PDF-"):
        logger.debug(f"Content from {url} does not start with PDF signature")
        return None

    return safely_extract_pdf_content(pdf_content)


def safely_extract_pdf_content(pdf_data: bytes) -> Optional[str]:
    """
    Extract text from in‑memory PDF bytes.

    Args:
        pdf_data: Raw PDF file content.

    Returns:
        Extracted text, or None if extraction failed.
    """
    if pdfplumber is None:
        return None

    try:
        with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
            pages = pdf.pages[:MAX_PDF_PAGES]
            texts = []
            for page_num, page in enumerate(pages, start=1):
                page_text = page.extract_text()
                if page_text:
                    texts.append(page_text)
            return "\n".join(texts) if texts else None
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
        return None