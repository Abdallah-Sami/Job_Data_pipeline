#!/usr/bin/env python3
"""
Wuzzuf job scraper -- production version for Docker / Azure.

Pipeline stage 01 (scrape). Runs incrementally:

  1. Loads the raw history from Azure (every job ever scraped).
  2. Pages through the Wuzzuf Saudi Arabia search results (newest first) and
     collects job URLs not in the history; stops at the first page that has
     nothing new.
  3. Downloads each new job page and extracts the raw `job` and `company`
     entities from the embedded `Wuzzuf.initialStoreState` JSON.
  4. Persists to Azure Data Lake Gen2 (via the Blob endpoint):
        raw/wuzzuf/wuzzuf_jobs_raw.json                       full history
        checkpoints/wuzzuf/wuzzuf_pending_raw_queue.json      handoff to cleaning
        checkpoints/wuzzuf/wuzzuf_failed_urls.txt             URLs that failed

No cleaning or normalization happens here; that is the cleaning stage's job.

Authentication (first match wins):
  * AZURE_STORAGE_CONNECTION_STRING    -- connection string
  * otherwise DefaultAzureCredential   -- Managed Identity (needs
    "Storage Blob Data Contributor"; set AZURE_CLIENT_ID for user-assigned).

Exit codes: 0 = success, 1 = incomplete / interrupted / nothing scraped,
            2 = fatal.
"""

import hashlib
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================

def _env_int(name, default):
    return int(os.getenv(name, default))


def _env_opt_int(name):
    """Integer env var; unset / empty / 0 means 'no limit' (None)."""
    value = os.getenv(name, "").strip()
    return int(value) if value and int(value) > 0 else None


def _env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# ---- Azure ----
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "jobpipelinedatalake")

# Format: "<container>/<blob path>"
WUZZUF_RAW_PATH = "raw/wuzzuf/wuzzuf_jobs_raw.json"
PENDING_RAW_QUEUE_PATH = "checkpoints/wuzzuf/wuzzuf_pending_raw_queue.json"
FAILED_URLS_PATH = "checkpoints/wuzzuf/wuzzuf_failed_urls.txt"

# ---- Site ----
SITE_ROOT = "https://wuzzuf.net"
SEARCH_BASE_URL = (
    "https://wuzzuf.net/saudi/search/jobs"
    "?filters%5Bcountry%5D%5B0%5D=Saudi%20Arabia"
)
SEARCH_PARAMS = {"q": "", "a": "spbg"}

# ---- Requests / retries ----
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 20)
MAX_RETRIES = _env_int("MAX_RETRIES", 3)
RETRY_BACKOFF_SECONDS = _env_int("RETRY_BACKOFF_SECONDS", 5)
LISTING_PAGE_DELAY = float(os.getenv("LISTING_PAGE_DELAY", "3.0"))
DETAIL_PAGE_DELAY = float(os.getenv("DETAIL_PAGE_DELAY", "0.5"))
WORKERS = _env_int("WORKERS", 4)

# ---- Run limits (unset = unlimited) ----
MAX_PAGES = _env_opt_int("MAX_PAGES")
MAX_JOBS = _env_opt_int("MAX_JOBS")

# ---- Behaviour ----
# True : skip jobs already in the raw history; stop paginating at the first
#        search page with nothing new.
# False: re-scrape everything found. Records are merged into the existing
#        history (newer version wins) -- the history is never wiped.
INCREMENTAL = _env_bool("INCREMENTAL", True)

# Each flush re-uploads the raw history and the pending queue, so keep this
# reasonably large (raise it for a big backfill).
JOBS_SAVE_EVERY = _env_int("JOBS_SAVE_EVERY", 100)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.5",
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8"
    ),
}

STORE_STATE_PATTERN = re.compile(
    r"Wuzzuf\.initialStoreState\s*=\s*(\{.*?\})\s*;\s*\n",
    re.DOTALL,
)
STORE_STATE_FALLBACK_PATTERN = re.compile(
    r"Wuzzuf\.initialStoreState\s*=\s*(\{.*\})",
    re.DOTALL,
)

RETRY_STATUSES = (403, 429, 500, 502, 503, 504)

log = logging.getLogger("wuzzuf_scraper")
shutdown_requested = threading.Event()


# ============================================================
# AZURE STORAGE
# ============================================================

class AzureStore:
    """Read/write blobs addressed as '<container>/<blob path>'."""

    def __init__(self, service):
        self.service = service

    @classmethod
    def from_env(cls):
        conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if conn:
            log.info("Azure auth: connection string")
            return cls(BlobServiceClient.from_connection_string(conn))

        from azure.identity import DefaultAzureCredential

        log.info("Azure auth: DefaultAzureCredential (managed identity)")
        return cls(
            BlobServiceClient(
                account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
                credential=DefaultAzureCredential(),
            )
        )

    def _blob(self, path):
        container, _, blob_name = path.partition("/")
        if not container or not blob_name:
            raise ValueError(f"Invalid storage path (need '<container>/<blob>'): {path!r}")
        return self.service.get_blob_client(container=container, blob=blob_name)

    def read_json(self, path, default):
        try:
            data = self._blob(path).download_blob().readall()
        except ResourceNotFoundError:
            return default
        return json.loads(data.decode("utf-8"))

    def write_json(self, path, obj):
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self._blob(path).upload_blob(payload, overwrite=True, max_concurrency=4)
        log.info("Uploaded %s (%.2f MB)", path, len(payload) / 1_048_576)

    def read_text(self, path, default=""):
        try:
            return self._blob(path).download_blob().readall().decode("utf-8")
        except ResourceNotFoundError:
            return default

    def write_text(self, path, text):
        self._blob(path).upload_blob(text.encode("utf-8"), overwrite=True)
        log.info("Uploaded %s", path)


# ============================================================
# HTTP HELPERS
# ============================================================

def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_html(session, url, params=None):
    """GET a URL with retries and backoff. Returns HTML text or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code != 200:
                log.warning(
                    "Non-200 (%s) for %s (attempt %d/%d)",
                    response.status_code, response.url, attempt, MAX_RETRIES,
                )
                if response.status_code in RETRY_STATUSES:
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    continue
                return None

            response.encoding = response.apparent_encoding or "utf-8"
            return response.text

        except requests.exceptions.RequestException as e:
            log.warning(
                "Request failed (%s) attempt %d/%d: %s", url, attempt, MAX_RETRIES, e
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    return None


# ============================================================
# STAGE 1: COLLECT JOB URLS
# ============================================================

def collect_job_urls(session, known_urls, max_pages=None):
    """
    Paginate search results (newest first) and collect unseen job URLs.
    Returns (job_urls, clean). clean=False means pagination was cut short by
    a failure or shutdown (URLs found so far are still returned).
    """
    job_urls = []
    seen = set()
    page = 0
    clean = True

    while True:
        if shutdown_requested.is_set():
            log.warning("Shutdown requested; stopping URL collection.")
            clean = False
            break

        if max_pages is not None and page >= max_pages:
            log.info("Reached MAX_PAGES=%d. Stopping URL collection.", max_pages)
            break

        params = dict(SEARCH_PARAMS)
        if page > 0:
            params["start"] = page

        log.info("Scraping search page %d...", page + 1)

        html = fetch_html(session, SEARCH_BASE_URL, params=params)
        if html is None:
            log.error("Search page %d failed after retries. Stopping.", page + 1)
            clean = False
            break

        soup = BeautifulSoup(html, "html.parser")
        page_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/jobs/p/" in href:
                url = urljoin(SITE_ROOT, href)
                url = url.replace("https://wuzzuf.net/ar/", "https://wuzzuf.net/")
                page_urls.append(url)

        new_this_run = [u for u in dict.fromkeys(page_urls) if u not in seen]
        unseen = [u for u in new_this_run if u not in known_urls]

        log.info(
            "  found %d links (%d new this run, %d not previously scraped)",
            len(page_urls), len(new_this_run), len(unseen),
        )

        if not new_this_run:
            if page == 0:
                # Page 1 is never legitimately empty: blocked / layout change.
                title = soup.title.get_text(strip=True) if soup.title else None
                log.error("Search page 1 returned NO job links -- likely blocked "
                          "or the page layout changed.")
                log.error("  Page title : %r", title)
                log.error("  HTML length: %d chars", len(html))
                log.error("  Body text  : %r", soup.get_text(" ", strip=True)[:500])
                clean = False
            else:
                log.info("No new links on this page at all. Stopping pagination.")
            break

        seen.update(new_this_run)

        if known_urls and not unseen:
            log.info("Every link on this page was already scraped in a previous "
                     "run. Stopping pagination (incremental).")
            break

        job_urls.extend(unseen)
        log.info("  running total: %d new unique job URLs", len(job_urls))

        page += 1
        time.sleep(LISTING_PAGE_DELAY)

    return job_urls, clean


# ============================================================
# STORE-STATE EXTRACTION
# ============================================================

def extract_store_state(html):
    """Extract and parse Wuzzuf.initialStoreState JSON from the page HTML."""
    match = STORE_STATE_PATTERN.search(html)
    if not match:
        match = STORE_STATE_FALLBACK_PATTERN.search(html)
    if not match:
        raise ValueError("Could not find 'Wuzzuf.initialStoreState' in the page HTML.")

    raw_json = match.group(1)
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        return json.loads(_trim_to_balanced_json(raw_json))


def _trim_to_balanced_json(text):
    start = text.find("{")
    if start == -1:
        raise ValueError("No opening brace found in store-state blob.")

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        char = text[i]

        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]

    raise ValueError("Could not find a balanced closing brace for the JSON blob.")


def generate_job_id(job_url):
    """Stable job_id from the Wuzzuf job URL slug."""
    slug_match = re.search(r"/jobs/p/([^/?]+)", job_url)
    if slug_match:
        return hashlib.sha1(slug_match.group(1).encode("utf-8")).hexdigest()[:12]
    return hashlib.sha1(job_url.encode("utf-8")).hexdigest()[:12]


def find_job_id_by_slug(store, url):
    """Match the correct job entity in the store state using the URL slug."""
    slug_match = re.search(r"/p/([^/?]+)", url)
    target_slug = slug_match.group(1) if slug_match else None

    job_collection = store.get("entities", {}).get("job", {}).get("collection", {})

    if target_slug:
        for job_id, job_entity in job_collection.items():
            if job_entity.get("attributes", {}).get("slug") == target_slug:
                return job_id, job_entity

    similar_jobs = store.get("jobPage", {}).get("similarJobs", {})
    if similar_jobs:
        main_job_id = next(iter(similar_jobs))
        job_entity = job_collection.get(main_job_id)
        if job_entity:
            return main_job_id, job_entity

    if job_collection:
        job_id = next(iter(job_collection))
        return job_id, job_collection[job_id]

    return None, None


def get_company(store, job_entity):
    company_ref = job_entity.get("relationships", {}).get("company", {}).get("data", {})
    if not company_ref:
        return None
    company_id = company_ref.get("id")
    return store.get("entities", {}).get("company", {}).get("collection", {}).get(company_id)


# ============================================================
# STAGE 2: SCRAPE ONE JOB PAGE (RAW ATTRIBUTES)
# ============================================================

def scrape_job_raw(session, url):
    """Fetch one job page and return its raw job/company attributes (or None)."""
    try:
        html = fetch_html(session, url)
        if html is None:
            log.error("Failed to download job page: %s", url)
            return None

        store_state = extract_store_state(html)

        wuzzuf_job_id, job_entity = find_job_id_by_slug(store_state, url)
        if not job_entity:
            log.error("Could not locate job entity in store state: %s", url)
            return None

        company_entity = get_company(store_state, job_entity)

        return {
            "job_id": generate_job_id(url),
            "job_url": url,
            "wuzzuf_job_id": wuzzuf_job_id,
            "job_attributes": job_entity.get("attributes", {}),
            "company_attributes": (company_entity or {}).get("attributes", {}),
            "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as e:
        log.error("Error parsing %s: %s: %s", url, type(e).__name__, e)
        return None


# ============================================================
# RUN
# ============================================================

def index_by_url(records):
    """{job_url: record}; later duplicates win (dedupe keep-last)."""
    return {r["job_url"]: r for r in records if r.get("job_url")}


def run_scraper(store):
    session = make_session()

    # The history is always loaded and preserved; INCREMENTAL only decides
    # whether known URLs are skipped.
    history_list = store.read_json(WUZZUF_RAW_PATH, [])
    history = index_by_url(history_list)
    if len(history) != len(history_list):
        log.warning("Raw history had %d record(s) without a unique job_url; "
                    "they are dropped on the next save.", len(history_list) - len(history))
    known_urls = set(history) if INCREMENTAL else set()

    if INCREMENTAL:
        log.info("Incremental mode: %d jobs already in raw history will be skipped.",
                 len(known_urls))
    else:
        log.info("Incremental mode off: re-scraping every job found "
                 "(history of %d jobs is preserved and merged).", len(history))

    pending = index_by_url(store.read_json(PENDING_RAW_QUEUE_PATH, []))

    log.info("=" * 70)
    log.info("STAGE 1: Collecting job URLs from search results")
    log.info("=" * 70)

    job_urls, discovery_clean = collect_job_urls(session, known_urls, max_pages=MAX_PAGES)

    # Retry URLs that failed last run. Pagination stops at the first fully-known
    # page, so a failed URL sitting behind it would otherwise never be revisited.
    previous_failed = [
        u.strip() for u in store.read_text(FAILED_URLS_PATH).splitlines() if u.strip()
    ]
    retry_urls = [u for u in previous_failed if u not in history]
    if retry_urls:
        log.info("Retrying %d URL(s) that failed in a previous run.", len(retry_urls))
    job_urls = list(dict.fromkeys(retry_urls + job_urls))

    if MAX_JOBS is not None:
        job_urls = job_urls[:MAX_JOBS]
        log.info("Limiting to first %d new jobs for detail scraping.", len(job_urls))

    if not job_urls:
        log.info("No new job URLs found. Nothing to scrape.")
        if previous_failed:
            store.write_text(FAILED_URLS_PATH, "")
        return dict(new=0, failed=0, attempted=0, history=len(history),
                    pending=len(pending), clean=discovery_clean, complete=True)

    log.info("=" * 70)
    log.info("STAGE 2: Scraping raw details for %d new jobs (workers=%d)",
             len(job_urls), WORKERS)
    log.info("=" * 70)

    new_count = 0
    failed_urls = []
    unflushed = []
    attempted = 0
    complete = True

    def worker(url):
        if shutdown_requested.is_set():
            return url, None, True  # skipped, not failed
        result = scrape_job_raw(make_session(), url)
        time.sleep(DETAIL_PAGE_DELAY)
        return url, result, False

    def flush():
        """
        Pending queue FIRST, raw history SECOND: a crash between the two only
        means those jobs are re-scraped (and de-duplicated) next run -- they
        can never end up in the history yet missing from the queue.
        """
        for rec in unflushed:
            pending[rec["job_url"]] = rec
            history[rec["job_url"]] = rec
        unflushed.clear()
        store.write_json(PENDING_RAW_QUEUE_PATH, list(pending.values()))
        store.write_json(WUZZUF_RAW_PATH, list(history.values()))

    # Records are only visible in `history` after flush(), so the raw file and
    # the queue are always written together.
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(worker, url): url for url in job_urls}

            for future in as_completed(futures):
                if shutdown_requested.is_set():
                    for f in futures:
                        f.cancel()
                    complete = False

                if future.cancelled():
                    continue

                url, result, skipped = future.result()
                if skipped:
                    complete = False
                    continue

                attempted += 1
                if result:
                    new_count += 1
                    unflushed.append(result)
                    title = (result.get("job_attributes") or {}).get("title") or url
                    log.info("[%d/%d] OK - %s", attempted, len(job_urls), title)
                else:
                    failed_urls.append(url)
                    log.info("[%d/%d] FAIL - %s", attempted, len(job_urls), url)

                if len(unflushed) >= JOBS_SAVE_EVERY:
                    flush()
                    log.info("  Checkpoint saved: %d in history, %d pending",
                             len(history), len(pending))
    finally:
        if unflushed:
            flush()

    if failed_urls or previous_failed:
        store.write_text(FAILED_URLS_PATH, "\n".join(failed_urls))
    if failed_urls:
        log.warning("%d job page(s) failed; they will be retried next run "
                    "(list saved to %s).", len(failed_urls), FAILED_URLS_PATH)

    return dict(new=new_count, failed=len(failed_urls), attempted=attempted,
                history=len(history), pending=len(pending),
                clean=discovery_clean, complete=complete)


# ============================================================
# MAIN
# ============================================================

def _handle_signal(signum, _frame):
    log.warning("Received signal %s; finishing current step and saving progress.", signum)
    shutdown_requested.set()


def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    started = time.time()
    store = AzureStore.from_env()
    r = run_scraper(store)

    log.info("=" * 60)
    log.info("New jobs scraped this run:      %d / %d attempted", r["new"], r["attempted"])
    log.info("Failed this run:                %d", r["failed"])
    log.info("Total jobs in raw history:      %d", r["history"])
    log.info("Jobs pending cleaning (queue):  %d", r["pending"])
    log.info("Elapsed: %.1f min", (time.time() - started) / 60)
    log.info("=" * 60)

    incomplete = (
        shutdown_requested.is_set()
        or not r["complete"]
        or not r["clean"]
        or (r["attempted"] > 0 and r["new"] == 0)  # everything failed
    )
    if incomplete:
        log.warning("Run finished incompletely; the next run will pick up where this left off.")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        logging.getLogger("wuzzuf_scraper").exception("Fatal error")
        sys.exit(2)
