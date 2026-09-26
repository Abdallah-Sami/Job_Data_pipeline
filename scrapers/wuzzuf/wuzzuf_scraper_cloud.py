"""
Wuzzuf Scraper - Cloud (Azure Data Lake) - Daily Incremental
=============================================================
Runs inside a container (ADF schedules it). Scrapes ONLY new jobs and writes
to Azure Data Lake using the Medallion architecture.

Data Lake layout:
  Container "landing":
    wuzzuf_<RUN_DATE>.json                          - jobs scraped in this run

  Container "checkpoints":
    wuzzuf/checkpoint.json                          - list of every URL ever scraped
    wuzzuf/wuzzuf_failed_<RUN_DATE>.json            - URLs that failed this run

Environment variables:
  AZURE_STORAGE_CONNECTION - storage account connection string
  RUN_DATE                 - date for filenames, e.g. "2026-09-21"
"""

import hashlib
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AZURE_CONN = os.environ["AZURE_STORAGE_CONNECTION"]
RUN_DATE = os.environ.get("RUN_DATE") or date.today().isoformat()

CONTAINER_LANDING = "landing"
CONTAINER_CHECKPOINTS = "checkpoints"

CHECKPOINT_BLOB = "wuzzuf/checkpoint.json"
DAILY_BLOB = f"wuzzuf_{RUN_DATE}.json"
FAILED_BLOB = f"wuzzuf/wuzzuf_failed_{RUN_DATE}.json"

SEARCH_BASE_URL = (
    "https://wuzzuf.net/saudi/search/jobs"
    "?filters%5Bcountry%5D%5B0%5D=Saudi%20Arabia"
)
SEARCH_PARAMS = {"q": "", "a": "spbg"}

REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
LISTING_PAGE_DELAY = 3.0
DETAIL_PAGE_DELAY = 0.5
WORKERS = 4
MAX_PAGES = int(os.environ.get("MAX_PAGES", "0")) or None
MAX_JOBS = int(os.environ.get("MAX_JOBS", "150")) or None

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
    r"Wuzzuf\.initialStoreState\s*=\s*(\{.*?\})\s*;\s*\n", re.DOTALL,
)
STORE_STATE_FALLBACK_PATTERN = re.compile(
    r"Wuzzuf\.initialStoreState\s*=\s*(\{.*\})", re.DOTALL,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logging.getLogger("azure").setLevel(logging.WARNING)
log = logging.getLogger("wuzzuf")

_blob_service = BlobServiceClient.from_connection_string(AZURE_CONN)


# ---------------------------------------------------------------------------
# Blob I/O
# ---------------------------------------------------------------------------
def read_json_blob(container, blob_name):
    blob = _blob_service.get_blob_client(container, blob_name)
    if not blob.exists():
        return None
    return json.loads(blob.download_blob().readall())


def write_json_blob(container, blob_name, obj):
    blob = _blob_service.get_blob_client(container, blob_name)
    blob.upload_blob(json.dumps(obj, ensure_ascii=False, indent=2), overwrite=True)


def load_checkpoint():
    data = read_json_blob(CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB)
    if data is None:
        return set()
    return set(data)


def save_checkpoint(url_set):
    write_json_blob(CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB, sorted(url_set))


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_html(session, url, params=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                log.warning("Non-200 (%s) for %s (attempt %d/%d)",
                            response.status_code, response.url, attempt, MAX_RETRIES)
                if response.status_code in (403, 429):
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    continue
                return None
            response.encoding = response.apparent_encoding or "utf-8"
            return response.text
        except requests.exceptions.RequestException as e:
            log.warning("Request failed (%s) attempt %d/%d: %s", url, attempt, MAX_RETRIES, e)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


# ---------------------------------------------------------------------------
# Stage 1 - Collect URLs
# ---------------------------------------------------------------------------
def collect_job_urls(session, known_urls):
    job_urls = []
    seen = set()
    page = 0

    # عدد الصفحات المتتالية التي لا تحتوي على وظائف جديدة
    # قبل أن نتوقف.
    MAX_CONSECUTIVE_KNOWN_PAGES = 9
    consecutive_known_pages = 0

    while True:

        if MAX_PAGES is not None and page >= MAX_PAGES:
            log.info(
                "Reached max_pages=%d. Stopping.",
                MAX_PAGES
            )
            break

        params = dict(SEARCH_PARAMS)

        if page > 0:
            params["start"] = page

        log.info(
            "Scraping search page %d...",
            page + 1
        )

        html = fetch_html(
            session,
            SEARCH_BASE_URL,
            params=params
        )

        # --------------------------------------------------
        # Request failed
        # --------------------------------------------------

        if html is None:
            log.error(
                "Search page %d failed. Stopping.",
                page + 1
            )
            break

        # --------------------------------------------------
        # Debug information
        # --------------------------------------------------

        log.info(
            "Search HTML length: %d",
            len(html)
        )

        log.info(
            "Contains /jobs/p/: %s",
            "/jobs/p/" in html
        )

        log.info(
            "Contains Wuzzuf: %s",
            "Wuzzuf" in html
        )

        # --------------------------------------------------
        # Parse HTML
        # --------------------------------------------------

        soup = BeautifulSoup(
            html,
            "html.parser"
        )

        page_urls = []

        for a in soup.find_all(
            "a",
            href=True
        ):

            href = a["href"]

            if "/jobs/p/" not in href:
                continue

            url = urljoin(
                "https://wuzzuf.net",
                href
            )

            url = url.replace(
                "https://wuzzuf.net/ar/",
                "https://wuzzuf.net/"
            )

            # Remove query parameters
            url = url.split("?")[0]

            # Remove fragments
            url = url.split("#")[0]

            page_urls.append(url)

        # --------------------------------------------------
        # Remove duplicates from current page
        # --------------------------------------------------

        page_urls = list(
            dict.fromkeys(page_urls)
        )

        # URLs not seen during THIS run
        new_this_run = [
            u
            for u in page_urls
            if u not in seen
        ]

        # URLs not present in the checkpoint
        unseen = [
            u
            for u in new_this_run
            if u not in known_urls
        ]

        log.info(
            "  found %d links (%d new this run, %d unseen)",
            len(page_urls),
            len(new_this_run),
            len(unseen)
        )

        # --------------------------------------------------
        # No links at all = end of pagination
        # --------------------------------------------------

        if not page_urls:

            log.info(
                "No job links found on page %d. "
                "End of pagination.",
                page + 1
            )

            break

        # --------------------------------------------------
        # Mark URLs seen during this run
        # --------------------------------------------------

        seen.update(
            new_this_run
        )

        # --------------------------------------------------
        # NEW LOGIC:
        # Do NOT stop just because this page
        # contains only already-known jobs.
        # --------------------------------------------------

        if unseen:

            # We found at least one job that is not
            # in the checkpoint.

            consecutive_known_pages = 0

            job_urls.extend(
                unseen
            )

            log.info(
                "  NEW jobs found on page %d: %d",
                page + 1,
                len(unseen)
            )

            log.info(
                "  running total: %d new URLs",
                len(job_urls)
            )

        else:

            consecutive_known_pages += 1

            log.info(
                "  No unseen jobs on page %d "
                "(%d/%d consecutive known pages)",
                page + 1,
                consecutive_known_pages,
                MAX_CONSECUTIVE_KNOWN_PAGES
            )

        # --------------------------------------------------
        # Stop only after several consecutive pages
        # contain no unseen jobs.
        # --------------------------------------------------

        if (
            consecutive_known_pages
            >= MAX_CONSECUTIVE_KNOWN_PAGES
        ):

            log.info(
                "Reached %d consecutive pages "
                "with no unseen jobs. "
                "Stopping incremental discovery.",
                MAX_CONSECUTIVE_KNOWN_PAGES
            )

            break

        # --------------------------------------------------
        # Optional maximum jobs
        # --------------------------------------------------

        if (
            MAX_JOBS is not None
            and len(job_urls) >= MAX_JOBS
        ):

            job_urls = job_urls[:MAX_JOBS]

            log.info(
                "Reached max_jobs=%d. Stopping.",
                MAX_JOBS
            )

            break

        # --------------------------------------------------
        # Next page
        # --------------------------------------------------

        page += 1

        time.sleep(
            LISTING_PAGE_DELAY
        )

    return job_urls


# ---------------------------------------------------------------------------
# Stage 2 - Extraction
# ---------------------------------------------------------------------------
def extract_store_state(html):
    match = STORE_STATE_PATTERN.search(html)
    if not match:
        match = STORE_STATE_FALLBACK_PATTERN.search(html)
    if not match:
        raise ValueError("Could not find 'Wuzzuf.initialStoreState'")

    raw_json = match.group(1)
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        return json.loads(_trim_to_balanced_json(raw_json))


def _trim_to_balanced_json(text):
    start = text.find("{")
    if start == -1:
        raise ValueError("No opening brace found.")
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
    raise ValueError("Could not find balanced closing brace.")


def generate_job_id(job_url):
    slug_match = re.search(r"/jobs/p/([^/?]+)", job_url)
    if slug_match:
        slug = slug_match.group(1)
        return hashlib.sha1(slug.encode("utf-8")).hexdigest()[:12]
    return hashlib.sha1(job_url.encode("utf-8")).hexdigest()[:12]


def find_job_id_by_slug(store, url):
    slug_match = re.search(r"/p/([^/?]+)", url)
    target_slug = slug_match.group(1) if slug_match else None
    job_collection = store.get("entities", {}).get("job", {}).get("collection", {})

    if target_slug:
        for job_id, job_entity in job_collection.items():
            attrs = job_entity.get("attributes", {})
            if attrs.get("slug") == target_slug:
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


def scrape_job_raw(session, url, run_stamp):
    try:
        html = fetch_html(session, url)
        if html is None:
            return None, "fetch_failed"

        store_state = extract_store_state(html)
        wuzzuf_job_id, job_entity = find_job_id_by_slug(store_state, url)
        if not job_entity:
            return None, "no_job_entity"

        company_entity = get_company(store_state, job_entity)

        job = {
            "job_id": generate_job_id(url),
            "job_url": url,
            "wuzzuf_job_id": wuzzuf_job_id,
            "job_attributes": job_entity.get("attributes", {}),
            "company_attributes": (company_entity or {}).get("attributes", {}),
            "collected_at": run_stamp,
            "batch_id": RUN_DATE,
        }
        return job, None

    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:100]}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    run_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    log.info("=" * 60)
    log.info("Wuzzuf scraper starting")
    log.info("  RUN_DATE:       %s", RUN_DATE)
    log.info("  Bronze target:  %s/%s", CONTAINER_LANDING, DAILY_BLOB)
    log.info("  Checkpoint:     %s/%s", CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB)
    log.info("=" * 60)

    session = make_session()
    known_urls = load_checkpoint()
    log.info("Checkpoint: %d URLs already scraped", len(known_urls))

    # Stage 1: Discover
    log.info("STAGE 1: Collecting job URLs")
    job_urls = collect_job_urls(session, known_urls)

    if MAX_JOBS is not None:
        job_urls = job_urls[:MAX_JOBS]

    if not job_urls:
        log.info("No new jobs. Writing empty bronze file.")
        write_json_blob(CONTAINER_LANDING, DAILY_BLOB, [])
        return

    log.info("STAGE 2: Scraping %d jobs (workers=%d)", len(job_urls), WORKERS)

    new_records = []
    failed_records = []

    def worker(url):
        thread_session = make_session()
        result, error = scrape_job_raw(thread_session, url, run_stamp)
        time.sleep(DETAIL_PAGE_DELAY)
        return url, result, error

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(worker, url): url for url in job_urls}
        done = 0

        for future in as_completed(futures):
            url, result, error = future.result()
            done += 1

            if result:
                new_records.append(result)
                known_urls.add(url)
                title = (result.get("job_attributes") or {}).get("title") or url
                log.info("[%d/%d] OK - %s", done, len(job_urls), title)
            else:
                failed_records.append({
                    "url": url,
                    "attempted_at": run_stamp,
                    "reason": error,
                })
                log.info("[%d/%d] FAIL - %s", done, len(job_urls), url)

            # Checkpoint every 50 jobs
            if done % 50 == 0:
                save_checkpoint(known_urls)
                log.info("Checkpoint saved (%d URLs)", len(known_urls))

    # Final writes
    save_checkpoint(known_urls)
    write_json_blob(CONTAINER_LANDING, DAILY_BLOB, new_records)
    log.info("Wrote %d jobs to %s/%s", len(new_records), CONTAINER_LANDING, DAILY_BLOB)

    if failed_records:
        write_json_blob(CONTAINER_CHECKPOINTS, FAILED_BLOB, failed_records)
        log.info("Wrote %d failed to %s/%s", len(failed_records), CONTAINER_CHECKPOINTS, FAILED_BLOB)

    log.info("=" * 60)
    log.info("Wuzzuf scraper finished")
    log.info("  New jobs:         %d", len(new_records))
    log.info("  Failed:           %d", len(failed_records))
    log.info("  Checkpoint total: %d URLs", len(known_urls))
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(1)
