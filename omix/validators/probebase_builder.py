"""
probeBase primer database builder.

Downloads the 16S primer list from probeBase, scrapes individual primer details,
and builds a searchable SQLite database.
"""

import csv
import re
import sqlite3
from pathlib import Path
from time import sleep
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

from omix.logging_utils import get_logger

logger = get_logger("omix.validators.builder")

PROBEBASE_LIST_URL = "https://probebase.net/lists/probes/"
PROBEBASE_SEARCH_URL = "https://probebase.net/search/results/reference/"
PROBEBASE_DETAIL_URL = "https://probebase.net/results/{primer_id}/"


def download_probebase_csv(session: requests.Session, save_path: Path) -> bool:
    """Download the 16S primer CSV from probeBase."""
    params = {
        'category': '',
        'target_rna': '16',
        'insitu': '',
        'is_primer': 'True',
        '_export': 'csv'
    }
    try:
        logger.info(f"Downloading data from {PROBEBASE_LIST_URL}...")
        response = session.get(PROBEBASE_LIST_URL, params=params, timeout=30)
        response.raise_for_status()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"✅ Downloaded to {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        return False


def get_primer_id_from_search(session: requests.Session, primer_name: str) -> Optional[str]:
    """Search probeBase for a primer ID by name."""
    if not primer_name:
        return None

    target_name = primer_name.strip()
    params = {'probename': target_name, 'link': 'or', 'filter': 'pcr'}
    try:
        response = session.get(PROBEBASE_SEARCH_URL, params=params, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # Strategy 1: exact link text match
        detail_links = soup.find_all('a', href=re.compile(r'/results/\d+/'))
        for link in detail_links:
            if link.get_text(strip=True).lower() == target_name.lower():
                match = re.search(r'/results/(\d+)/', link['href'])
                if match:
                    logger.debug(f"Found exact link for '{target_name}'")
                    return match.group(1)

        # Strategy 2: pattern match in parent element
        search_pattern = re.compile(r'\b' + re.escape(target_name) + r'\b', re.IGNORECASE)
        primer_text_node = soup.find(string=search_pattern)
        if primer_text_node:
            parent = primer_text_node.find_parent()
            if parent:
                link = parent.find('a', href=re.compile(r'/results/\d+/'))
                if link and 'href' in link.attrs:
                    match = re.search(r'/results/(\d+)/', link['href'])
                    if match:
                        logger.debug(f"Found linked ID for '{target_name}'")
                        return match.group(1)

        logger.warning(f"Could not find link for '{target_name}'")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error searching '{target_name}': {e}")
        return None


def get_primer_details(session: requests.Session, primer_id: str, primer_name: str) -> Optional[Dict[str, Any]]:
    """Scrape detail page for a primer."""
    detail_url = PROBEBASE_DETAIL_URL.format(primer_id=primer_id)
    details = {"Primer_Name": primer_name, "ProbeBase_ID": f"pB-{primer_id}"}

    try:
        response = session.get(detail_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        header = soup.find('strong', string='Accession no.')
        if not header:
            return details  # return what we have

        table = header.find_parent('table')
        if not table:
            return details

        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(separator=' ', strip=True).replace(';', ',')
                details[key] = value

        return details
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching details for ID {primer_id}: {e}")
        return None


def build_primer_database_direct(session: requests.Session, csv_path: Path, db_path: Path) -> bool:
    """Read primers from CSV, scrape details, and write directly to SQLite."""
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            primer_names = sorted(list(set([row[0].strip() for row in reader if row and row[0].strip()])))
    except (IOError, csv.Error) as e:
        logger.error(f"Failed to read CSV: {e}")
        return False

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS primers')

    schema = (
        "Primer_Name TEXT, ProbeBase_ID TEXT, Sequence TEXT, Length TEXT, "
        "Position TEXT, Direction TEXT, Accession_no TEXT, References TEXT, "
        "Position_Start INTEGER, Position_End INTEGER, Target TEXT"
    )
    cursor.execute(f'CREATE TABLE primers ({schema})')

    insert_sql = '''INSERT INTO primers 
                    (Primer_Name, ProbeBase_ID, Sequence, Length, Position, 
                     Direction, Accession_no, References, Position_Start, Position_End, Target) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    total_primers = len(primer_names)
    logger.info(f"Found {total_primers} unique primers. Scraping directly to SQLite...")
    successful = 0

    for name in primer_names:
        primer_id = get_primer_id_from_search(session, name)
        if primer_id:
            details = get_primer_details(session, primer_id, name)
            if details:
                pos_val = details.get("Position", "")
                start, end = None, None
                match = re.match(r'(\d+)\s*-\s*(\d+)', pos_val)
                if match:
                    start, end = int(match.group(1)), int(match.group(2))

                target = details.get("Target", "")
                cursor.execute(insert_sql, (
                    details.get("Primer_Name"),
                    details.get("ProbeBase_ID"),
                    details.get("Sequence"),
                    details.get("Length"),
                    pos_val,
                    details.get("Direction"),
                    details.get("Accession no."),
                    details.get("References"),
                    start,
                    end,
                    target,
                ))
                successful += 1
        sleep(0.1)

    logger.info("Creating database indexes...")
    cursor.execute('CREATE INDEX idx_pos_start ON primers (Position_Start)')
    cursor.execute('CREATE INDEX idx_pos_end ON primers (Position_End)')
    cursor.execute('CREATE INDEX idx_direction ON primers (Direction)')

    conn.commit()
    conn.close()
    logger.info(f"✅ Built database with {successful} primers at {db_path}")
    return db_path.exists()


def import_and_save_database(
    csv_path: Path = Path("data/probe_data.csv"),
    db_path: Path = Path("data/primer_data.db")
)-> bool:
    """Full workflow: download CSV, scrape, build SQLite DB."""
    if db_path.exists():
        logger.info(f"✅ Database already exists at '{db_path}'. Skipping import.")
        return True

    logger.info("Database not found. Starting scrape and import...")
    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36"
        })

        if not download_probebase_csv(session, csv_path):
            logger.critical("Failed to download initial data. Aborting.")
            return False

        if not build_primer_database_direct(session, csv_path, db_path):
            logger.critical("Failed to build primer database from CSV. Aborting.")
            return False

        if csv_path.exists():
            csv_path.unlink()

    return db_path.exists()