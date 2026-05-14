"""
Webpage text extraction using BeautifulSoup.

Provides a function to fetch a webpage, remove boilerplate / navigation,
and return the main article text.
"""

import re
from typing import Optional

import requests
from bs4 import BeautifulSoup

from omix.logging_utils import get_logger

logger = get_logger("omix.extractors.webpage")


def extract_text_from_webpage(
    url: str, session: Optional[requests.Session] = None
) -> Optional[str]:
    """
    Fetch a webpage and extract the main text content.

    Args:
        url: URL of the article / publication page.
        session: Optional requests.Session for connection pooling.

    Returns:
        Cleaned text string, or None if the page could not be retrieved.
    """
    req = session.get if session else requests.get
    try:
        response = req(url, timeout=25)
        response.raise_for_status()
    except requests.RequestException:
        logger.debug(f"Failed to fetch webpage: {url}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the main content area
    main = soup.find("article") or soup.find("main") or soup.body
    if not main:
        logger.debug(f"No article/main/body tag found on {url}")
        return None

    # Convert links pointing to supplementary / table / data into plain text
    # markers so the LLM can see them.
    for tag in main.find_all("a"):
        link_text = tag.get_text(strip=True).lower()
        if any(k in link_text for k in ["table", "supp", "si", "file", "data"]):
            tag.replace_with(
                f" [LINK: {tag.get_text(strip=True)} - {tag.get('href')}] "
            )
        else:
            tag.decompose()

    text = main.get_text(separator=" ", strip=True)

    # Strip out common boilerplate patterns (journal headers, navigation)
    boilerplate_patterns = [
        r"skip to main content",
        r"an official website of the united states government",
        r"here's how you know",
        r"search log in dashboard",
        r"publications account settings",
        r"search in pmc",
        r"search in pubmed",
        r"view in nlm catalog",
        r"add to search",
        r"user guide",
        r"permalink copy",
        r"pmc disclaimer",
        r"pmc copyright notice",
        r"the author\\(s\\)",
        r"find articles by",
        r"author information article notes copyright and license information",
    ]
    for pattern in boilerplate_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) > 100 else None