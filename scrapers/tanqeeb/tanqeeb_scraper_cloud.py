"""
Tanqeeb Scraper - Cloud (Azure Data Lake) - Daily Incremental
==============================================================
Runs inside a container (ADF schedules it). Scrapes ONLY new jobs and writes
to Azure Data Lake using the Medallion architecture.

Both discovery (search result pages) and detail scraping use plain
`requests` — no browser/Selenium needed. Verified that the search page
serves full job listings server-side (no JS required) and that job detail
pages contain all needed fields directly in the initial HTML response.

Data Lake layout:
  Container "landing":
    tanqeeb_<RUN_DATE>.json                            - jobs scraped in this run

  Container "checkpoints":
    tanqeeb/checkpoint.json                            - list of every URL ever scraped
    tanqeeb/tanqeeb_failed_<RUN_DATE>.json             - URLs that failed this run

Environment variables:
  AZURE_STORAGE_CONNECTION - storage account connection string
  RUN_DATE                 - date for filenames, e.g. "2026-09-21"
"""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AZURE_CONN = os.environ["AZURE_STORAGE_CONNECTION"]
RUN_DATE = os.environ.get("RUN_DATE") or date.today().isoformat()

CONTAINER_LANDING = "landing"
CONTAINER_CHECKPOINTS = "checkpoints"

CHECKPOINT_BLOB = "tanqeeb/checkpoint.json"
# Time suffix makes every run write its own file, so a second run on the same
# day can never overwrite (and lose) the first run's data.
DAILY_BLOB = f"tanqeeb_{RUN_DATE}_{datetime.now():%H%M%S}.json"
FAILED_BLOB = f"tanqeeb/tanqeeb_failed_{RUN_DATE}.json"

BASE_URL = "https://saudi.tanqeeb.com"
SEARCH_PATH = f"{BASE_URL}/jobs/search"
QUERY_STRING = (
    "keywords=&country=54&state=0&category=-1&"
    "workplace=0&search_period=0&lang=all&change_lang=1"
)
SOURCE_NAME = "Tanqeeb"

# Only URLs on this exact host are ever queued as job/pagination candidates.
# Fixes a bug where broad fallback selectors (see get_job_links_from_html)
# picked up footer/nav links to other country subdomains (egypt.tanqeeb.com,
# uae.tanqeeb.com, bahrain.tanqeeb.com, ...) once real Saudi listings ran
# out, ballooning the discovery queue into the thousands.
ALLOWED_NETLOC = "saudi.tanqeeb.com"

# Discovery (requests — no browser needed)
INCREMENTAL_MODE = True
STOP_AFTER_CONSECUTIVE_KNOWN_PAGES = int(os.environ.get("STOP_AFTER_CONSECUTIVE_KNOWN_PAGES", "80"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "1000"))
PAGE_DELAY = 1

# Scraping (requests)
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1
BATCH_SIZE = 50
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Optional proxy — kept for emergencies (e.g. if IP-blocking resumes), off by
# default now that plain requests has been confirmed to work without one.
PROXY_ENABLED = os.environ.get("PROXY_ENABLED", "false").lower() == "true"
PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = os.environ.get("PROXY_PORT", "")
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
REQUESTS_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_ENABLED else None

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
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("tanqeeb")

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
# Discovery helpers (plain requests — no browser needed)
# ---------------------------------------------------------------------------
def build_page_url(page_num):
    if page_num <= 1:
        return f"{SEARCH_PATH}?{QUERY_STRING}"
    return f"{SEARCH_PATH}/page/{page_num}?{QUERY_STRING}"


def get_job_links_from_html(html):
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "h2.search-job-title a.search-job-title-link",
        "h2 a[href*='/jobs/']",
        "h3 a[href*='/jobs/']",
        "a[href*='/jobs/']",
    ]

    for selector in selectors:
        links = soup.select(selector)

        if links:
            log.info(
                "Job selector matched: %s (%d links)",
                selector,
                len(links)
            )
            return links

    # Diagnostic information
    all_links = soup.find_all("a", href=True)

    log.warning(
        "No job selector matched. Total links on page: %d",
        len(all_links)
    )

    sample = []

    for link in all_links[:100]:
        href = link.get("href", "")
        text = link.get_text(" ", strip=True)

        if href:
            sample.append({
                "text": text[:100],
                "href": href[:200]
            })

    log.warning("Sample links: %s", sample)

    return []


def discover_new_urls(known_urls, session):
    """Paginate search results with plain requests, return list of new URLs."""
    new_urls = []
    queued = set()
    known_pages = 0
    page_num = 1
    page_retries = 0
    MAX_PAGE_RETRIES = 3

    while page_num <= MAX_PAGES:
        page_url = build_page_url(page_num)
        log.info("Loading search page %d", page_num)

        try:
            resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            html = resp.text
            page_retries = 0
        except (requests.RequestException, OSError) as e:
            # OSError also catches low-level SSL/socket errors (e.g. ssl.SSLError,
            # ConnectionResetError) that requests/urllib3 sometimes let through
            # unwrapped — without this, a single transient network blip would
            # crash the whole script before any progress got saved.
            page_retries += 1
            log.warning(
                "Request error on page %d (attempt %d/%d): %s",
                page_num, page_retries, MAX_PAGE_RETRIES, e,
            )
            if page_retries >= MAX_PAGE_RETRIES:
                log.warning(
                    "Giving up on page %d after %d attempts. Stopping discovery "
                    "with %d URLs queued so far.",
                    page_num, page_retries, len(new_urls),
                )
                break
            time.sleep(PAGE_DELAY * 2)
            continue

        job_links = get_job_links_from_html(html)
        log.info("  Job links found: %d", len(job_links))

        if not job_links:
            log.info("No more listings. Stopping.")
            break

        new_on_page = 0
        for link in job_links:
            href = link.get("href")
            if not href:
                continue
            url = urljoin(BASE_URL, href)

            # Reject anything not on saudi.tanqeeb.com — the broad fallback
            # selectors above can match footer/nav links to other country
            # subdomains once real listings run out. Without this check the
            # discovery queue balloons into the thousands with irrelevant
            # non-Saudi pages.
            if urlparse(url).netloc != ALLOWED_NETLOC:
                continue

            if INCREMENTAL_MODE and url in known_urls:
                continue
            if url not in queued:
                queued.add(url)
                new_urls.append(url)
                new_on_page += 1

        if new_on_page:
            known_pages = 0
            log.info("  New jobs: %d (total queued: %d)", new_on_page, len(new_urls))
        else:
            known_pages += 1
            log.info("  No new jobs (%d/%d)", known_pages, STOP_AFTER_CONSECUTIVE_KNOWN_PAGES)
            if not INCREMENTAL_MODE or known_pages >= STOP_AFTER_CONSECUTIVE_KNOWN_PAGES:
                log.info("Caught up. Stopping.")
                break

        page_num += 1
        time.sleep(PAGE_DELAY)

    return new_urls


# ---------------------------------------------------------------------------
# Scraping helpers (requests)
# ---------------------------------------------------------------------------
def make_session():
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update(REQUEST_HEADERS)
    if REQUESTS_PROXIES:
        session.proxies.update(REQUESTS_PROXIES)
    return session


def _text(element):
    return element.get_text(" ", strip=True) if element else None


def _abs_url(element, attr):
    value = element.get(attr) if element else None
    return urljoin(BASE_URL, value) if value else None


WORK_MODES = {"on-site", "remote", "hybrid"}
JOB_TYPES = {"full time", "part time", "contract", "internship", "temporary", "freelance"}


def parse_job_page(html, url, run_stamp):
    soup = BeautifulSoup(html, "html.parser")

    apply_button = soup.select_one(".apply-btn")
    job_id = apply_button.get("data-job-id") if apply_button else None
    apply_url = (apply_button.get("href") or apply_button.get("data-url")) if apply_button else None

    company_el = soup.select_one(
        "a.job-meta-company, .job-company-name, .company-name, a.company-name-link"
    )
    company_logo_el = soup.select_one(".company-logo img, .job-company-logo img")

    tags = [t.get_text(" ", strip=True)
            for t in soup.select(".job-tags .job-tag") if t.get_text(strip=True)]
    work_mode = job_type = None
    for tag in tags:
        if tag.lower() in WORK_MODES:
            work_mode = tag
        elif tag.lower() in JOB_TYPES:
            job_type = tag

    date_el = soup.select_one(".job-date")
    posted_date = None
    if date_el and date_el.get("data-datetime"):
        try:
            from dateutil.parser import parse as parse_date
            posted_date = parse_date(date_el["data-datetime"]).isoformat()
        except Exception:
            posted_date = date_el["data-datetime"]

    details = {}
    for row in soup.select(".meta-data .meta"):
        spans = row.find_all("span")
        if len(spans) >= 2:
            details[spans[0].get_text(" ", strip=True)] = spans[1].get_text(" ", strip=True)

    skills_list = [s.get_text(" ", strip=True)
                   for s in soup.select(".job-skills .skill, .skills-list .skill-tag")
                   if s.get_text(strip=True)]

    return {
        "id": job_id,
        "source": SOURCE_NAME,
        "title": _text(soup.select_one("h3.job-title-text")),
        "company": _text(company_el),
        "company_url": _abs_url(company_el, "href"),
        "company_logo_url": _abs_url(company_logo_el, "src"),
        "description": _text(soup.select_one("#jobDescriptionBody")),
        "url": url,
        "apply_url": apply_url,
        "location": _text(soup.select_one(".job-meta-item span")),
        "work_mode": work_mode,
        "job_type": details.get("Employment") or job_type,
        "career_level": details.get("Career Level"),
        "experience": details.get("Experience"),
        "education": details.get("Education"),
        "gender": details.get("Gender"),
        "industry": details.get("Industry"),
        "skills": "; ".join(skills_list) if skills_list else None,
        "salary": details.get("Salary"),
        "posted_date": posted_date,
        "collected_at": run_stamp,
        "batch_id": RUN_DATE,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    run_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    log.info("=" * 60)
    log.info("Tanqeeb scraper starting")
    log.info("  RUN_DATE:       %s", RUN_DATE)
    log.info("  Proxy enabled:  %s (%s:%s)", PROXY_ENABLED, PROXY_HOST if PROXY_ENABLED else "-", PROXY_PORT if PROXY_ENABLED else "-")
    log.info("  Bronze target:  %s/%s", CONTAINER_LANDING, DAILY_BLOB)
    log.info("  Checkpoint:     %s/%s", CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB)
    log.info("=" * 60)

    known_urls = load_checkpoint()
    log.info("Checkpoint: %d URLs already scraped", len(known_urls))

    session = make_session()

    # Stage 1: Discovery (requests — no browser needed)
    log.info("STAGE 1: Discovering new job URLs")
    new_urls = discover_new_urls(known_urls, session)
    log.info("Discovered %d new URLs", len(new_urls))

    MAX_JOBS_PER_RUN = int(os.environ.get("MAX_JOBS_PER_RUN", "100")) or None
    if MAX_JOBS_PER_RUN and len(new_urls) > MAX_JOBS_PER_RUN:
        log.info(
            "Capping this run to %d jobs (out of %d discovered) via MAX_JOBS_PER_RUN. "
            "Remaining will be picked up on the next run.",
            MAX_JOBS_PER_RUN, len(new_urls),
        )
        new_urls = new_urls[:MAX_JOBS_PER_RUN]

    if not new_urls:
        log.info("No new jobs. Writing empty bronze file.")
        write_json_blob(CONTAINER_LANDING, DAILY_BLOB, [])
        return

    # Stage 2: Scrape details (requests)
    log.info("STAGE 2: Scraping %d job pages", len(new_urls))

    new_records = []
    failed_records = []
    stats = {"scraped": 0, "gone": 0, "failed": 0}

    try:
        for i, url in enumerate(new_urls, start=1):
            log.info("[%d/%d] %s", i, len(new_urls), url)

            try:
                response = session.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                log.warning("  Request error: %s", e)
                failed_records.append({"url": url, "attempted_at": run_stamp, "reason": str(e)[:100]})
                stats["failed"] += 1
                continue

            if response.status_code in (404, 410):
                log.info("  Gone (%d), marking permanently.", response.status_code)
                known_urls.add(url)
                stats["gone"] += 1
            elif response.status_code != 200:
                log.warning("  HTTP %d (will retry next run)", response.status_code)
                failed_records.append({"url": url, "attempted_at": run_stamp, "reason": f"http_{response.status_code}"})
                stats["failed"] += 1
                continue
            else:
                try:
                    job = parse_job_page(response.text, url, run_stamp)
                except Exception as e:
                    log.warning("  Parse error: %s (will retry)", e)
                    failed_records.append({"url": url, "attempted_at": run_stamp, "reason": f"parse_error: {str(e)[:80]}"})
                    stats["failed"] += 1
                    continue
                new_records.append(job)
                known_urls.add(url)
                stats["scraped"] += 1
                log.info("  OK: %s", job.get("title"))

            # Checkpoint every BATCH_SIZE
            if i % BATCH_SIZE == 0:
                # Write the landing file BEFORE the checkpoint, so a URL is only
                # ever marked "known" once its data is safely in landing. Even a
                # hard kill (closed window, PC sleep, power loss) can no longer
                # leave the checkpoint ahead of the data.
                write_json_blob(CONTAINER_LANDING, DAILY_BLOB, new_records)
                save_checkpoint(known_urls)
                log.info("  Saved %d jobs to landing + checkpoint (%d URLs)",
                         len(new_records), len(known_urls))

            time.sleep(REQUEST_DELAY)
    finally:
        # Always save progress, even on crash / Ctrl-C — including the
        # landing file and failed-records file, so an interrupted run
        # doesn't lose whatever was already scraped before it was stopped.
        save_checkpoint(known_urls)

        write_json_blob(CONTAINER_LANDING, DAILY_BLOB, new_records)
        log.info("Wrote %d jobs to %s/%s", len(new_records), CONTAINER_LANDING, DAILY_BLOB)

        if failed_records:
            write_json_blob(CONTAINER_CHECKPOINTS, FAILED_BLOB, failed_records)
            log.info("Wrote %d failed to %s/%s", len(failed_records), CONTAINER_CHECKPOINTS, FAILED_BLOB)

    log.info("=" * 60)
    log.info("Tanqeeb scraper finished")
    log.info("  Scraped:         %d", stats["scraped"])
    log.info("  Gone (404/410):  %d", stats["gone"])
    log.info("  Failed:          %d", stats["failed"])
    log.info("  Checkpoint total: %d URLs", len(known_urls))
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(1)
