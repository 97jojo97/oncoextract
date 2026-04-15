"""PubMed E-utilities client for fetching oncology abstracts."""

import json
import logging
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from oncoextract.db.models import get_engine

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DEFAULT_QUERY = "Nasopharyngeal Carcinoma"
BATCH_SIZE = 200
MAX_RESULTS = 5000
MIN_REQUEST_INTERVAL = 0.34  # ~3 requests/sec with API key


class PubMedClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("PUBMED_API_KEY", "")
        self.session = requests.Session()
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict:
        # Omit empty api_key — NCBI may return 400; unauthenticated tier still works (lower rate).
        if self.api_key:
            params["api_key"] = self.api_key
        params["retmode"] = "json"
        url = f"{BASE_URL}/{endpoint}"

        for attempt in range(5):
            self._throttle()
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited, backing off %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"Failed after 5 retries: {endpoint}")

    def search_ids(self, query: str = DEFAULT_QUERY, max_results: int = MAX_RESULTS) -> list[str]:
        """Return a list of PMIDs matching the query."""
        all_ids: list[str] = []
        retstart = 0

        while retstart < max_results:
            batch = min(BATCH_SIZE, max_results - retstart)
            data = self._get("esearch.fcgi", {
                "db": "pubmed",
                "term": query,
                "retmax": batch,
                "retstart": retstart,
            })
            ids = data.get("esearchresult", {}).get("idlist", [])
            if not ids:
                break
            all_ids.extend(ids)
            retstart += len(ids)
            logger.info("Fetched %d/%d PMIDs", len(all_ids), max_results)

        return all_ids

    def fetch_details(self, pmids: list[str]) -> list[dict]:
        """Fetch article details for a batch of PMIDs (max ~200 at a time)."""
        results: list[dict] = []
        for i in range(0, len(pmids), BATCH_SIZE):
            batch = pmids[i : i + BATCH_SIZE]
            self._get("efetch.fcgi", {
                "db": "pubmed",
                "id": ",".join(batch),
                "rettype": "abstract",
                "retmode": "xml",
            })
            summary = self._get("esummary.fcgi", {
                "db": "pubmed",
                "id": ",".join(batch),
            })
            for pmid in batch:
                article = summary.get("result", {}).get(pmid, {})
                if article and isinstance(article, dict):
                    results.append({"pmid": pmid, **article})
            logger.info("Fetched details for %d/%d articles", len(results), len(pmids))

        return results

    def fetch_abstracts(self, pmids: list[str]) -> list[dict]:
        """Fetch full abstract text via efetch XML and parse it."""
        results: list[dict] = []
        for i in range(0, len(pmids), BATCH_SIZE):
            batch = pmids[i : i + BATCH_SIZE]
            articles = None
            for attempt in range(3):
                try:
                    self._throttle()
                    resp = self.session.get(
                        f"{BASE_URL}/efetch.fcgi",
                        params={
                            "db": "pubmed",
                            "id": ",".join(batch),
                            "rettype": "abstract",
                            "retmode": "xml",
                            "api_key": self.api_key,
                        },
                        timeout=90,
                    )
                    resp.raise_for_status()
                    articles = _parse_pubmed_xml(resp.text)
                    break
                except (requests.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                    wait = 2 ** (attempt + 1)
                    logger.warning("Batch %d failed (attempt %d): %s. Retrying in %ds",
                                   i // BATCH_SIZE, attempt + 1, e, wait)
                    time.sleep(wait)
            if articles is None:
                logger.error("Skipping batch at offset %d after 3 retries", i)
                continue
            results.extend(articles)
            logger.info("Fetched abstracts for %d/%d articles", len(results), len(pmids))

        return results


def _parse_pubmed_xml(xml_text: str) -> list[dict]:
    """Parse PubMed XML into structured dicts. Uses stdlib xml.etree."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_text)
    articles = []

    for article_el in root.findall(".//PubmedArticle"):
        medline = article_el.find("MedlineCitation")
        if medline is None:
            continue

        pmid_el = medline.find("PMID")
        pmid = pmid_el.text if pmid_el is not None else None
        if not pmid:
            continue

        article = medline.find("Article")
        if article is None:
            continue

        title_el = article.find("ArticleTitle")
        title = title_el.text if title_el is not None else ""

        abstract_parts = []
        abstract_el = article.find("Abstract")
        if abstract_el is not None:
            for at in abstract_el.findall("AbstractText"):
                label = at.get("Label", "")
                text_content = "".join(at.itertext())
                if label:
                    abstract_parts.append(f"{label}: {text_content}")
                else:
                    abstract_parts.append(text_content)
        abstract_text = "\n".join(abstract_parts)

        authors = []
        author_list = article.find("AuthorList")
        if author_list is not None:
            for author in author_list.findall("Author"):
                last = author.findtext("LastName", "")
                first = author.findtext("ForeName", "")
                if last:
                    authors.append(f"{last}, {first}".strip(", "))

        journal_el = article.find("Journal/Title")
        journal = journal_el.text if journal_el is not None else ""

        pub_date_el = article.find("Journal/JournalIssue/PubDate")
        pub_date = None
        if pub_date_el is not None:
            year = pub_date_el.findtext("Year", "")
            month = pub_date_el.findtext("Month", "01")
            day = pub_date_el.findtext("Day", "01")
            if year:
                month_map = {
                    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
                }
                month = month_map.get(month, month)
                try:
                    pub_date = f"{year}-{int(month):02d}-{int(day):02d}"
                except (ValueError, TypeError):
                    pub_date = f"{year}-01-01"

        mesh_terms = []
        mesh_list = medline.find("MeshHeadingList")
        if mesh_list is not None:
            for mesh in mesh_list.findall("MeshHeading/DescriptorName"):
                if mesh.text:
                    mesh_terms.append(mesh.text)

        articles.append({
            "pmid": pmid,
            "title": title,
            "abstract_text": abstract_text,
            "authors": authors,
            "journal": journal,
            "pub_date": pub_date,
            "mesh_terms": mesh_terms,
        })

    return articles


def ingest_to_postgres(
    query: str = DEFAULT_QUERY,
    max_results: int = MAX_RESULTS,
) -> int:
    """Run the full ingestion: search PMIDs, fetch details, store in Postgres."""
    client = PubMedClient()
    engine = get_engine()

    logger.info("Searching PubMed for: %s (max %d)", query, max_results)
    pmids = client.search_ids(query, max_results)
    logger.info("Found %d PMIDs", len(pmids))

    if not pmids:
        return 0

    # Check which PMIDs we already have
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT pmid FROM raw_pubmed WHERE pmid = ANY(:ids)"),
            {"ids": pmids},
        ).scalars().all()
    existing_set = set(existing)
    new_pmids = [p for p in pmids if p not in existing_set]
    logger.info("Skipping %d already-ingested, fetching %d new", len(existing_set), len(new_pmids))

    if not new_pmids:
        return 0

    inserted = 0
    for i in range(0, len(new_pmids), BATCH_SIZE):
        batch_pmids = new_pmids[i : i + BATCH_SIZE]
        articles = client.fetch_abstracts(batch_pmids)

        with engine.begin() as conn:
            for article in articles:
                conn.execute(
                    text("""
                        INSERT INTO raw_pubmed (pmid, raw_json)
                        VALUES (:pmid, :raw_json)
                        ON CONFLICT (pmid) DO NOTHING
                    """),
                    {"pmid": article["pmid"], "raw_json": json.dumps(article)},
                )
                inserted += 1

        logger.info("Committed batch %d: %d/%d total inserted",
                     i // BATCH_SIZE + 1, inserted, len(new_pmids))

    logger.info("Inserted %d records into raw_pubmed", inserted)
    return inserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = ingest_to_postgres()
    print(f"Ingested {count} articles")
