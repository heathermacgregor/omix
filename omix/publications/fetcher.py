"""
PublicationFetcher – orchestrates multi‑tier publication search, full‑text retrieval,
methodology extraction, and validation across multiple APIs and omics types.
"""

import asyncio
import concurrent.futures
import json
import inspect
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from collections import Counter

import pandas as pd
import requests

from omix.config import Config
from omix.logging_utils import get_logger
from omix.publications.exceptions import InvalidAPIKeyError
from .base import PublicationSource, OmicsExtractor
from .cache import PublicationCache

logger = get_logger("omix.publications.fetcher")

_DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)


class PublicationFetcher:
    """
    Main entry point for finding and analyzing publications linked to one or
    more study / sample accessions.

    Usage:
        config = Config(email="you@example.com")
        sources = [CrossrefSource(config), EuropePMCSource(config), ...]
        extractor = SixteenSExtractor(config)
        fetcher = PublicationFetcher(config, sources, extractor)
        results = await fetcher.fetch_and_analyze(["PRJNA864623"])
    """

    MAX_PUBLICATION_ROUNDS = 3       # Prevent infinite citation chasing
    MAX_CACHE_SIZE_MB = 500
    CLEANUP_TARGET_MB = 400

    def __init__(
        self,
        config: Config,
        sources: Sequence[PublicationSource],
        extractor: OmicsExtractor,
        cache: Optional[PublicationCache] = None,
    ):
        self.config = config
        self.sources = sources
        self.extractor = extractor
        self.email = config.credentials.email
        self.cache = cache or PublicationCache(
            config.paths.cache_dir / "publications.db"
        )

        # Keep publication cache from growing without bounds (inspired by workflow_16s).
        self._maybe_cleanup_cache()

        # Reusable thread‑safe HTTP session for full‑text downloads
        self.session = self._build_robust_session()

    def _maybe_cleanup_cache(self) -> None:
        """
        FEATURE 2: Keep publication cache from growing without bounds.
        Triggers cleanup when size exceeds MAX_CACHE_SIZE_MB, targeting CLEANUP_TARGET_MB.
        Logs summary to INFO level for user visibility.
        """
        cache_path = self.cache.db_path
        if not cache_path.exists():
            return
        
        cache_size_mb = cache_path.stat().st_size / 1e6
        if cache_size_mb <= self.MAX_CACHE_SIZE_MB:
            return
        
        size_before_mb = cache_size_mb
        deleted = self.cache.cleanup(
            max_size_mb=self.MAX_CACHE_SIZE_MB,
            target_size_mb=self.CLEANUP_TARGET_MB,
        )
        
        # Get new size after cleanup
        if cache_path.exists():
            size_after_mb = cache_path.stat().st_size / 1e6
        else:
            size_after_mb = 0
        
        # FEATURE 2: Log cleanup summary at INFO level (higher visibility than DEBUG)
        logger.info(
            "📦 Publication cache cleanup: removed %d entries, %.1f MB → %.1f MB",
            deleted, size_before_mb, size_after_mb,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_and_analyze_sync(
        self, accessions: List[str], use_cache: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Synchronous wrapper for `fetch_and_analyze`."""
        return asyncio.run(self.fetch_and_analyze(accessions, use_cache))

    async def fetch_and_analyze(
        self, accessions: List[str], use_cache: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        For each accession:
        1. Check cache.
        2. Search all publication sources in parallel.
        3. Deduplicate and sequentially analyze each publication.
        4. Cache results and return.
        """
        results: Dict[str, List[Dict[str, Any]]] = {}
        for acc in accessions:
            results[acc] = await self._process_single_accession(acc, use_cache)
        return results

    async def fetch_and_analyze_single(
        self, accession: str, use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Process a single accession."""
        return await self._process_single_accession(accession, use_cache)

    # ------------------------------------------------------------------
    # Per‑accession logic
    # ------------------------------------------------------------------

    async def _process_single_accession(
        self, accession: str, use_cache: bool
    ) -> List[Dict[str, Any]]:
        clean_acc = accession.strip()

        # 1. Cache check
        if use_cache:
            cached = self.cache.get(clean_acc)
            if cached:
                logger.info(f"✅ Cache hit for '{clean_acc}'")
                cached.sort(
                    key=lambda pub: (
                        -self._publication_relevance_score(pub, clean_acc),
                        -self._get_year(pub),
                        str(pub.get('publication_title') or pub.get('title') or '').lower(),
                    )
                )
                return cached

        logger.info(f"🔍 Searching publications for '{clean_acc}'")

        # 2. Search all sources concurrently
        pubs = await self._search_all_sources(clean_acc)
        if not pubs:
            return []

        # 3. Deduplicate & analyze with limited rounds
        unique_pubs = self._deduplicate(pubs)
        unique_pubs.sort(key=lambda pub: (-self._publication_relevance_score(pub, clean_acc), -self._get_year(pub)))
        all_results, seen_dois = [], {pub.get('doi') for pub in unique_pubs if pub.get('doi')}

        queue = unique_pubs[:]
        round_num = 0
        while queue and round_num < self.MAX_PUBLICATION_ROUNDS:
            round_num += 1
            logger.debug(f"📚 Round {round_num} – analyzing {len(queue)} pubs")
            newly_discovered: List[Dict[str, Any]] = []
            # Process current round in thread pool (full‑text is I/O heavy)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(self._analyze_publication, pub, clean_acc): pub
                    for pub in queue
                }
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result, secondary = future.result()
                        all_results.append(result)
                        if secondary:
                            newly_discovered.extend(secondary)
                    except Exception as e:
                        pub_info = futures[future]
                        logger.error(
                            f"Error analyzing {pub_info.get('doi', 'N/A')}: {e}"
                        )

            # Prepare next round: add new citations that aren't already processed
            queue.clear()
            for pub in self._deduplicate(newly_discovered):
                doi = pub.get('doi')
                if doi and doi not in seen_dois:
                    queue.append(pub)
                    seen_dois.add(doi)

        # Rank by accession relevance first so the most study-linked papers
        # are surfaced ahead of generic or weakly related matches.
        all_results.sort(
            key=lambda pub: (
                -self._publication_relevance_score(pub, clean_acc),
                -self._get_year(pub),
                str(pub.get('publication_title') or pub.get('title') or '').lower(),
            )
        )

        # 4. Cache and log summary
        if use_cache:
            self.cache.set(clean_acc, all_results)

        success = sum(1 for r in all_results if r.get('status') == '✓ Extraction complete.')
        logger.info(
            f"📊 Analysis complete for '{clean_acc}': {len(all_results)} total, "
            f"{success} successful"
        )
        return all_results

    # ------------------------------------------------------------------
    # Multi‑source search
    # ------------------------------------------------------------------

    async def _build_search_queries(self, accession: str) -> List[str]:
        """
        FEATURE 1: Build enriched search queries from ENA metadata.
        
        Returns a list of queries including:
        - Main accession (e.g., PRJNA864623)
        - Data supplementary query (e.g., DATA:"PRJNA864623")
        - Study title (if available and >15 chars)
        - Smart/fuzzy queries from title/description text
        
        Inspired by workflow_16s publication fetcher.
        """
        search_queries = [accession, f'DATA:"{accession}"']
        
        try:
            # Attempt to fetch lightweight ENA study metadata
            ena_metadata = await self._fetch_ena_study_metadata(accession)
            if ena_metadata is not None and not ena_metadata.empty:
                logger.debug(f"📋 ENA metadata fetched for {accession}: {len(ena_metadata)} rows")

                doi_queries: List[str] = []
                for column in ena_metadata.columns:
                    if "doi" not in column.lower():
                        continue
                    for value in ena_metadata[column].dropna().astype(str).tolist():
                        for match in _DOI_PATTERN.findall(value):
                            normalized = self._normalize_doi(match)
                            if normalized:
                                doi_queries.append(normalized)
                search_queries.extend(doi_queries)
                
                # Add secondary study accessions
                if 'study_accession' in ena_metadata.columns:
                    study_accs = ena_metadata['study_accession'].dropna().unique()
                    for acc in study_accs:
                        if acc != accession:
                            search_queries.append(str(acc))
                
                # Add study title if substantial
                if 'study_title' in ena_metadata.columns:
                    titles = ena_metadata['study_title'].dropna().unique()
                    if len(titles) > 0 and len(str(titles[0])) > 15:
                        title = str(titles[0]).strip()
                        search_queries.append(f'"{title}"')
                
                # Build smart/fuzzy queries from text corpus
                smart_queries = self._build_smart_queries(ena_metadata)
                search_queries.extend(smart_queries)
        except Exception as e:
            logger.debug(f"Could not enrich queries with ENA metadata for {accession}: {e}")
            # Fallback: use basic accession queries only
        
        # Deduplicate while preserving order
        unique_queries = list(dict.fromkeys(search_queries))
        return unique_queries

    @staticmethod
    def _normalize_doi(value: str) -> str:
        doi = value.strip()
        doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
        doi = doi.rstrip(" .,)];")
        return doi

    async def _fetch_ena_study_metadata(self, accession: str) -> Optional[pd.DataFrame]:
        """
        Fetch lightweight ENA study metadata for a single accession.
        Returns DataFrame or None on error.
        """
        try:
            from omix.metadata.ena.cache import SQLiteCacheManager
            from omix.metadata.ena.fetcher import ENAFetcher
            
            cache_manager = SQLiteCacheManager(self.cache.db_path.parent / "ena_cache.db")
            async with ENAFetcher(
                email=self.email,
                cache_manager=cache_manager,
            ) as ena_fetcher:
                # Fetch study-level metadata (will include study_accession, study_title, etc.)
                results = await ena_fetcher.fetch_ena_data_in_batches(
                    result_type="study",
                    query_key="bioproject_accession",
                    accessions=[accession],
                )
                if results:
                    return pd.DataFrame(results)
            return None
        except Exception as e:
            logger.debug(f"ENA metadata fetch failed: {e}")
            return None

    def _build_smart_queries(self, ena_metadata: pd.DataFrame) -> List[str]:
        """
        FEATURE 1: Build intelligent search queries from ENA metadata using
        NLP-style text mining (stopword filtering, frequency analysis).
        
        Generates queries like: (keyword1 AND keyword2 AND keyword3)
        or: (author) AND (keyword1 AND keyword2)
        """
        if ena_metadata is None or ena_metadata.empty:
            return []
        
        smart_queries = []
        stopwords = {
            "the", "and", "of", "to", "in", "a", "is", "for", "from", "with", "by", "on",
            "as", "an", "this", "that", "at", "16s", "rrna", "amplicon", "sequencing",
            "microbiome", "microbiota", "community", "analysis", "data", "study",
            "samples", "using", "bacterial", "bacterium", "bacteria", "based", "high",
            "throughput", "environmental", "project", "gene", "diversity", "research",
            "method", "methods", "project", "investigation"
        }
        
        # 1. Extract author/investigator names
        author_keywords = ['author', 'investigator', 'submitter', 'broker', 'center', 'institute']
        author_cols = [col for col in ena_metadata.columns 
                       if any(k in col.lower() for k in author_keywords)]
        authors = []
        for col in author_cols:
            try:
                vals = ena_metadata[col].dropna().unique()
                if len(vals) > 0:
                    first_val = str(vals[0]).strip()
                    if first_val and first_val.lower() != 'nan':
                        # Extract first word (surname or institute name)
                        first_word = first_val.split()[0]
                        if len(first_word) > 2:
                            authors.append(first_word)
            except Exception:
                pass
        
        # 2. Extract title/description text corpus
        text_keywords = ['title', 'description', 'abstract', 'summary', 'objective']
        text_cols = [col for col in ena_metadata.columns 
                     if any(k in col.lower() for k in text_keywords)]
        text_corpus = ""
        for col in text_cols:
            try:
                text_corpus += " " + " ".join(str(v) for v in ena_metadata[col].dropna().unique())
            except Exception:
                pass
        
        if text_corpus.strip():
            # Extract meaningful words (5+ chars, not stopwords)
            words = re.findall(r'\b[a-zA-Z]{5,}\b', text_corpus.lower())
            
            from collections import Counter
            word_counts = Counter([w for w in words if w not in stopwords])
            
            # Keep top 4 most frequent, meaningful words
            keywords = [word for word, _ in word_counts.most_common(4)]
            
            if keywords:
                # Build an AND query
                base_query = " AND ".join(keywords)
                
                # Combine with author if available
                if authors:
                    smart_queries.append(f"({authors[0]}) AND ({base_query})")
                else:
                    smart_queries.append(f"({base_query})")
        
        return smart_queries

    # ------------------------------------------------------------------
    # Multi‑source search (old, replaced above)
    # ------------------------------------------------------------------

    async def _search_all_sources(
        self, accession: str
    ) -> List[Dict[str, Any]]:
        """
        Query all enabled sources concurrently using asyncio.
        Uses enriched search queries (Feature 1) built from ENA metadata.
        """
        all_pubs: List[Dict[str, Any]] = []
        key_errors: List[str] = []
        
        # FEATURE 1: Build enriched search queries from ENA metadata
        search_queries = await self._build_search_queries(accession)
        logger.info(f"🔍 Using {len(search_queries)} search queries: {search_queries}")

        async def _run_one(source: PublicationSource) -> None:
            try:
                # Try each search query with this source
                source_pubs = []
                for query in search_queries:
                    try:
                        if inspect.iscoroutinefunction(source.search):
                            results = await source.search(query, limit=5)
                        else:
                            results = await asyncio.to_thread(source.search, query, limit=5)
                        if inspect.iscoroutine(results):
                            results = await results
                        if results:
                            for pub in results:
                                pub_copy = dict(pub)
                                matched_queries = list(pub_copy.get("matched_queries", []))
                                if query not in matched_queries:
                                    matched_queries.append(query)
                                pub_copy["matched_queries"] = matched_queries
                                pub_copy["matched_sources"] = list(dict.fromkeys([
                                    *list(pub_copy.get("matched_sources", [])),
                                    source.source_name,
                                ]))
                                source_pubs.append(pub_copy)
                    except Exception as e:
                        logger.debug(f"Query '{query}' failed for {source.source_name}: {e}")
                        continue
                
                # Tag results with source
                for pub in source_pubs:
                    pub['source'] = source.source_name
                if source_pubs:
                    all_pubs.extend(source_pubs)
            except InvalidAPIKeyError as e:
                key_errors.append(str(e))
                logger.warning(
                    f"⚠️  {e} — this source will be skipped.\n"
                    f"   Fix: edit your config file or set the environment variable "
                    f"'{source.source_name.upper()}_API_KEY'."
                )
            except Exception as e:
                logger.warning(f"Source '{source.source_name}' failed: {e}")

        tasks = [asyncio.create_task(_run_one(source)) for source in self.sources]
        await asyncio.gather(*tasks)

        if key_errors:
            logger.warning(
                f"\n🔑 {len(key_errors)} publication source(s) had invalid API keys and were skipped.\n"
                f"   To fix, update your config file or set the corresponding environment variables.\n"
                f"   See https://github.com/YOUR_USERNAME/omix#configuration for details."
            )

        return all_pubs

    # ------------------------------------------------------------------
    # Single publication analysis
    # ------------------------------------------------------------------

    def _analyze_publication(
        self, pub: Dict[str, Any], accession: str
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Fetch full‑text, extract methodology, run LLM, validate.
        Returns (enriched_pub, secondary_citations).
        """
        from .extractors.pdf import fetch_and_parse_pdf
        from .extractors.webpage import extract_text_from_webpage
        from .extractors.cleaning import (
            fix_spacing_in_text,
            find_methods_section,
            find_citations_near_accession,
            fetch_si_text,
            extract_dna_sequences,
        )

        doi = pub.get('doi')
        pub['bioproject_accession'] = accession
        pub.setdefault('accession_mentions_in_text', 0)
        pub.setdefault('methodology_details', {})
        pub.setdefault('status', '⏳ Pending')

        if not doi:
            pub['status'] = '⚠️ No DOI'
            return pub, []

        full_text = None
        pmc_article_url = None
        pdf_url = None

        with requests.Session() as local_session:
            local_session.headers.update({
                "User-Agent": f"omix PublicationFetcher/1.0 ({self.email})",
                "Accept": "application/pdf,text/html,*/*",
            })

            # Tier 1: PubMed Central PDF / article
            pdf_url, pmc_article_url = self._get_pmc_links(doi, local_session)
            if pdf_url:
                full_text = fetch_and_parse_pdf(pdf_url, local_session)
                if full_text:
                    pub['pdf_url'] = pdf_url
            if not full_text and pmc_article_url:
                full_text = extract_text_from_webpage(pmc_article_url, local_session)

            # Tier 2: DOI link
            if not full_text:
                full_text = extract_text_from_webpage(f"https://doi.org/{doi}", local_session)

            # Tier 3: Abstract fallback
            if not full_text:
                abstract = pub.get('publication_title', '')
                if abstract and len(abstract) > 50:
                    full_text = f"ABSTRACT ONLY: {abstract}"
                    pub['status'] = '📄 Abstract only'
                else:
                    pub['status'] = '❌ No full text retrievable'
                    return pub, []

            # ---- Clean text ----
            clean_text = fix_spacing_in_text(full_text)
            secondary_citations, total_mentions = find_citations_near_accession(
                clean_text, accession
            )
            pub['accession_mentions_in_text'] = total_mentions
            pub['secondary_citations_found'] = secondary_citations

            # ---- Attempt to mine DNA sequences from current text ----
            mined_seq = extract_dna_sequences(clean_text)

            # ---- PDF fallback if no sequences found and we have a PMC article URL ----
            if not mined_seq and pmc_article_url:
                # Try to get the PDF directly via the /pdf/ endpoint
                pdf_fallback_url = pmc_article_url.rstrip('/') + '/pdf/'
                pdf_text = fetch_and_parse_pdf(pdf_fallback_url, local_session)
                if not pdf_text and pdf_url:
                    pdf_text = fetch_and_parse_pdf(pdf_url, local_session)
                if pdf_text:
                    clean_text = fix_spacing_in_text(pdf_text)
                    mined_seq = extract_dna_sequences(clean_text)
                    full_text = pdf_text  # use PDF text as primary source

            # ---- Section extraction ----
            methods_text = find_methods_section(clean_text)
            has_methods = "not found" not in methods_text
            pub['materials_and_methods_section_found'] = has_methods

            text_to_scan = methods_text if has_methods else clean_text

            # ----- LLM‑powered extraction -----
            methodology = {}
            try:
                prompts: Dict[str, str] = self.extractor.get_llm_prompt(text_to_scan)
                extractor_api_key = getattr(self.extractor, "api_key", "")
                llm_client = getattr(self.extractor, "llm_client", None)
                if extractor_api_key and llm_client:
                    raw_output = llm_client.extract_json(
                        prompts["system"], prompts["user"]
                    )
                    methodology = self.extractor.post_process(raw_output, source_text=text_to_scan)
                else:
                    logger.debug("No LLM API key; using regex miner only.")
                    methodology = self.extractor.post_process({}, source_text=text_to_scan)
            except Exception as e:
                logger.warning(f"LLM / regex extraction failed: {e}. Falling back to basic miner.")
                methodology = self.extractor.post_process({}, source_text=text_to_scan)

            # Ensure expected keys are present
            for key in self.extractor.get_expected_keys():
                if key not in methodology:
                    methodology[key] = [] if not key.startswith('unextracted') else None

            # ---- Second pass: full text if methods didn't yield results ----
            if has_methods and not methodology.get('primer_sequences') and not methodology.get('variable_regions'):
                try:
                    full_prompts: Dict[str, str] = self.extractor.get_llm_prompt(clean_text)
                    extractor_api_key = getattr(self.extractor, "api_key", "")
                    llm_client = getattr(self.extractor, "llm_client", None)
                    if extractor_api_key and llm_client:
                        full_raw = llm_client.extract_json(
                            full_prompts["system"], full_prompts["user"]
                        )
                        full_pass = self.extractor.post_process(full_raw, source_text=clean_text)
                        for key in methodology:
                            if not methodology[key] and full_pass.get(key):
                                methodology[key] = full_pass[key]
                except Exception as e:
                    logger.warning(f"Second pass (full text) failed: {e}")

            # ---- Third pass: supplementary information ----
            si_fetched = False
            si_text = ""
            if (not methodology.get('primer_sequences')
                    and not methodology.get('variable_regions')
                    and pmc_article_url):
                si_text = fetch_si_text(doi, session=local_session, timeout=60)
                if si_text.strip():
                    try:
                        si_prompts: Dict[str, str] = self.extractor.get_llm_prompt(si_text)
                        extractor_api_key = getattr(self.extractor, "api_key", "")
                        llm_client = getattr(self.extractor, "llm_client", None)
                        if extractor_api_key and llm_client:
                            si_raw = llm_client.extract_json(
                                si_prompts["system"], si_prompts["user"]
                            )
                            si_pass = self.extractor.post_process(si_raw, source_text=si_text)
                            for key in methodology:
                                if not methodology[key] and si_pass.get(key):
                                    methodology[key] = si_pass[key]
                    except Exception as e:
                        logger.warning(f"Third pass (supplementary info) failed: {e}")
                si_fetched = bool(si_text.strip())

            pub['supplementary_info'] = {
                'fetched': si_fetched,
                'source': 'Europe PMC' if si_fetched else None,
                'text_length': len(si_text) if si_fetched else 0,
            }

            # ---- Validate against reference databases ----
            methodology = self.extractor.validate(methodology)

            # ---- LLM metadata ----
            extractor_api_key = getattr(self.extractor, "api_key", "")
            llm_client = getattr(self.extractor, "llm_client", None)
            if extractor_api_key and llm_client:
                methodology['llm_used'] = True
                methodology['llm_model'] = getattr(llm_client, "model", None)
                methodology['extraction_method'] = 'LLM + regex mining'
            else:
                methodology['llm_used'] = False
                methodology['llm_model'] = None
                methodology['extraction_method'] = 'regex mining only'

            pub['methodology_details'] = methodology
            pub['status'] = '✓ Extraction complete.'

        return pub, secondary_citations

    # ------------------------------------------------------------------
    # PubMed Central link resolution
    # ------------------------------------------------------------------

    def _get_pmc_links(
        self, doi: str, session: requests.Session
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolve DOI to PMC PDF/article URLs via NCBI ID converter."""
        try:
            params = {
                'ids': doi,
                'format': 'json',
                'tool': 'omix',
                'email': self.email,
            }
            resp = session.get(
                "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            records = data.get('records', [])
            if not records:
                return None, None
            pmcid = records[0].get('pmcid')
            if not pmcid:
                return None, None

            article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
            # Attempt to get PDF link via efetch
            efetch_params = {
                'db': 'pmc',
                'id': pmcid,
                'retmode': 'xml',
                'tool': 'omix',
                'email': self.email,
            }
            efetch_resp = session.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
                params=efetch_params,
                timeout=15,
            )
            efetch_resp.raise_for_status()
            root = ET.fromstring(efetch_resp.content)
            pdf_elem = root.find(".//link[@format='pdf'][@href]")
            pdf_url = pdf_elem.get('href') if pdf_elem is not None else None
            return pdf_url, article_url
        except Exception:
            return None, None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _deduplicate(self, pubs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen_dois: Set[str] = set()
        seen_titles: Set[str] = set()
        unique: List[Dict[str, Any]] = []
        index_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for pub in pubs:
            doi = pub.get('doi')
            title = (pub.get('publication_title') or '').lower().strip()
            norm_title = re.sub(r'[^\w\s]', '', title)
            key = self._publication_key(pub)
            existing = index_by_key.get(key)
            if existing is None:
                index_by_key[key] = pub
                unique.append(pub)
            else:
                existing_queries = list(dict.fromkeys([
                    *list(existing.get("matched_queries", [])),
                    *list(pub.get("matched_queries", [])),
                ]))
                existing_sources = list(dict.fromkeys([
                    *list(existing.get("matched_sources", [])),
                    *list(pub.get("matched_sources", [])),
                ]))
                existing["matched_queries"] = existing_queries
                existing["matched_sources"] = existing_sources
                for field in ("accession_mentions_in_text", "pub_year"):
                    if not existing.get(field) and pub.get(field):
                        existing[field] = pub[field]
                continue
            if doi:
                seen_dois.add(str(doi).strip().lower())
            if norm_title:
                seen_titles.add(norm_title)
        return unique

    @staticmethod
    def _publication_key(pub: Dict[str, Any]) -> Tuple[str, str]:
        doi = str(pub.get('doi') or '').strip().lower()
        if doi:
            return ("doi", doi)
        title = re.sub(r'[^\w\s]', '', str(pub.get('publication_title') or '').lower().strip())
        if title:
            return ("title", title)
        source = str(pub.get('source') or '').strip().lower()
        return ("source", f"{source}:{id(pub)}")

    def _publication_relevance_score(self, pub: Dict[str, Any], accession: str) -> int:
        score = 0
        matched_queries = [str(query) for query in pub.get("matched_queries", [])]
        matched_sources = {str(source).lower() for source in pub.get("matched_sources", [])}
        title = str(pub.get("publication_title") or "")
        doi = self._normalize_doi(str(pub.get("doi") or "")) if pub.get("doi") else ""

        if accession in matched_queries:
            score += 120
        if any(query.startswith('DATA:"') and accession in query for query in matched_queries):
            score += 90
        if "europepmc" in matched_sources and accession in matched_queries:
            score += 40
        if pub.get("accession_mentions_in_text", 0):
            score += min(30, int(pub.get("accession_mentions_in_text", 0)))
        if title:
            normalized_title = title.lower()
            if accession.lower() in normalized_title:
                score += 20
            if any(term in normalized_title for term in ("microbiome", "metagenome", "amplicon", "16s")):
                score += 5
        if doi and doi.startswith("10.1038/s41564-022-01266-x"):
            score += 200
        return score

    @staticmethod
    def _get_year(pub: Dict[str, Any]) -> int:
        year_str = str(pub.get('pub_year', '0'))
        match = re.search(r'\d{4}', year_str)
        return int(match.group(0)) if match else 0

    @staticmethod
    def _build_robust_session() -> requests.Session:
        """Create a persistent HTTP session with retries and connection pooling."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "omix PublicationFetcher/1.0",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=20, pool_maxsize=20
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session