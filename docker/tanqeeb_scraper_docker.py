#!/usr/bin/env python3
"""
Tanqeeb job scraper -- production version for Docker / Azure Container Instances.

Pipeline stage 01 (scrape). Runs incrementally:

  1. Loads the master jobs checkpoint from Azure (every job ever scraped).
  2. Walks Tanqeeb search pages (Selenium) from page 1, newest first, collecting
     job URLs not seen before, and stops once it has caught up with known jobs.
  3. Scrapes each new job page (requests + BeautifulSoup).
  4. Persists to Azure Data Lake Gen2 (via the Blob endpoint):
        raw/tanqeeb/tanqeeb_jobs_raw.json                        full history
        checkpoints/tanqeeb/tanqeeb_jobs_checkpoint.json         master job list
        checkpoints/tanqeeb/tanqeeb_new_urls_checkpoint.json     in-flight URLs
        checkpoints/tanqeeb/tanqeeb_pending_raw_queue.json       handoff to cleaning

Authentication (first match wins):
  * AZURE_STORAGE_CONNECTION_STRING    -- connection string (store as a secure env var)
  * otherwise DefaultAzureCredential   -- Managed Identity on ACI (recommended).
    The identity needs "Storage Blob Data Contributor" on the storage account.
    For a user-assigned identity also set AZURE_CLIENT_ID.

Optional environment overrides: see the CONFIG section.

Exit codes: 0 = success, 1 = completed with problems or interrupted, 2 = fatal.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import ReadTimeoutError

# ============================================================
# CONFIG
# ============================================================

def _env_int(name, default):
    return int(os.getenv(name, default))


def _env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# ---- Azure ----
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "jobpipelinedatalake")

# Format: "<container>/<blob path>"
TANQEEB_RAW_PATH = "raw/tanqeeb/tanqeeb_jobs_raw.json"
JOBS_CHECKPOINT_PATH = "checkpoints/tanqeeb/tanqeeb_jobs_checkpoint.json"
NEW_URLS_CHECKPOINT_PATH = "checkpoints/tanqeeb/tanqeeb_new_urls_checkpoint.json"
PENDING_RAW_QUEUE_PATH = "checkpoints/tanqeeb/tanqeeb_pending_raw_queue.json"

# ---- Site ----
BASE_URL = "https://saudi.tanqeeb.com"
SEARCH_PATH = "https://saudi.tanqeeb.com/jobs/search"
QUERY_STRING = (
    "keywords=&country=54&state=0&category=-1&"
    "workplace=0&search_period=0&lang=all&change_lang=1"
)
SOURCE_NAME = "Tanqeeb"

# ---- Behaviour ----
# Each flush re-uploads the full jobs checkpoint, so keep this reasonably large
# (raise it further for a full-site backfill).
JOBS_SAVE_EVERY = _env_int("JOBS_SAVE_EVERY", 100)
INCREMENTAL_MODE = _env_bool("INCREMENTAL_MODE", True)
STOP_AFTER_CONSECUTIVE_KNOWN_PAGES = _env_int("STOP_AFTER_CONSECUTIVE_KNOWN_PAGES", 2)
MAX_PAGES = _env_int("MAX_PAGES", 2000)
MAX_CONSECUTIVE_FAILURES = _env_int("MAX_CONSECUTIVE_FAILURES", 5)
PAGE_LOAD_WAIT_SECONDS = _env_int("PAGE_LOAD_WAIT_SECONDS", 5)
# Extra wait (max) for job cards to appear; slow containers need this
WAIT_FOR_RESULTS_SECONDS = _env_int("WAIT_FOR_RESULTS_SECONDS", 30)
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "1"))
REQUEST_TIMEOUT_SECONDS = _env_int("REQUEST_TIMEOUT_SECONDS", 30)
REQUEST_MAX_ATTEMPTS = _env_int("REQUEST_MAX_ATTEMPTS", 3)

# ---- Chrome (paths set in the Dockerfile; None => Selenium Manager) ----
CHROME_BIN = os.getenv("CHROME_BIN")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH")

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    )
}

log = logging.getLogger("tanqeeb_scraper")
shutdown_requested = threading.Event()


# ============================================================
# AZURE STORAGE
# ============================================================

class AzureJsonStore:
    """Read/write JSON blobs addressed as '<container>/<blob path>'."""

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

    def exists(self, path):
        return self._blob(path).exists()

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

    def delete(self, path):
        try:
            self._blob(path).delete_blob()
        except ResourceNotFoundError:
            pass


# ============================================================
# HELPERS
# ============================================================

def format_datetime(dt):
    """Format as e.g. '11/9/2026 4:54:00 PM' (no leading zeros on day/month/hour)."""
    if dt is None or pd.isna(dt):
        return "N/A"

    hour_12 = dt.hour % 12 or 12
    am_pm = "AM" if dt.hour < 12 else "PM"
    return f"{dt.day}/{dt.month}/{dt.year} {hour_12}:{dt.minute:02d}:{dt.second:02d} {am_pm}"


def build_page_url(page_num):
    """Page 1: /jobs/search?...   Page N: /jobs/search/page/N?..."""
    if page_num <= 1:
        return f"{SEARCH_PATH}?{QUERY_STRING}"
    return f"{SEARCH_PATH}/page/{page_num}?{QUERY_STRING}"


def get_job_links_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select("h2.search-job-title a.search-job-title-link")


def make_driver():
    """Headless Chrome configured for running inside a container."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Required in containers
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument(f"--user-agent={HTTP_HEADERS['User-Agent']}")
    if CHROME_BIN:
        options.binary_location = CHROME_BIN

    service = Service(executable_path=CHROMEDRIVER_PATH) if CHROMEDRIVER_PATH else Service()
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(120)
    try:
        driver.command_executor.set_timeout(300)
    except Exception:  # API differs between Selenium versions
        pass
    return driver


def safe_quit(driver):
    try:
        driver.quit()
    except Exception:
        pass


def known_urls_from_jobs(jobs):
    return {
        j.get("job_url")
        for j in jobs
        if j.get("job_url") and j.get("job_url") != "N/A"
    }


# ============================================================
# PHASE 1: DISCOVER NEW JOB URLS
# ============================================================

def discover_new_urls(store, known_job_urls, new_job_urls):
    """
    Walk search pages from page 1. Appends to `new_job_urls` (list, order kept)
    and persists it after every page. Returns True if pagination ended cleanly,
    False if it was aborted (driver failures / shutdown).
    """
    queued = set(new_job_urls)
    driver = make_driver()
    consecutive_failures = 0
    consecutive_known_pages = 0
    page_num = 1
    clean = True

    try:
        while page_num <= MAX_PAGES:
            if shutdown_requested.is_set():
                log.warning("Shutdown requested; stopping pagination.")
                clean = False
                break

            page_url = build_page_url(page_num)
            log.info("Loading search page %d: %s", page_num, page_url)

            try:
                driver.get(page_url)
                time.sleep(PAGE_LOAD_WAIT_SECONDS)  # let JS render results
                try:
                    WebDriverWait(driver, WAIT_FOR_RESULTS_SECONDS).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "h2.search-job-title a.search-job-title-link")
                        )
                    )
                except TimeoutException:
                    pass  # legit on the last page; handled below
                html = driver.page_source
                consecutive_failures = 0
            except (TimeoutException, WebDriverException, ReadTimeoutError) as e:
                consecutive_failures += 1
                log.error(
                    "Driver error on page %d (failure %d/%d): %s",
                    page_num, consecutive_failures, MAX_CONSECUTIVE_FAILURES, e,
                )
                safe_quit(driver)
                time.sleep(3)

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    log.error("Too many consecutive driver failures; stopping pagination.")
                    clean = False
                    driver = None
                    break

                driver = make_driver()
                continue  # retry the same page

            job_links = get_job_links_from_html(html)
            log.info("  Job links found on page: %d", len(job_links))

            if not job_links:
                if page_num == 1:
                    # Page 1 is never legitimately empty: blocked / not rendered.
                    soup = BeautifulSoup(html, "html.parser")
                    log.error("Page 1 returned NO job links -- likely blocked or not rendered.")
                    log.error("  Page title : %r", driver.title)
                    log.error("  HTML length: %d chars", len(html))
                    log.error("  Body text  : %r", soup.get_text(" ", strip=True)[:500])
                    clean = False
                    break
                log.info("No more job listings found. Stopping pagination.")
                break

            new_on_this_page = 0
            for link in job_links:
                href = link.get("href")
                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)
                if INCREMENTAL_MODE and full_url in known_job_urls:
                    continue
                if full_url not in queued:
                    queued.add(full_url)
                    new_job_urls.append(full_url)
                    new_on_this_page += 1

            if INCREMENTAL_MODE:
                if new_on_this_page == 0:
                    consecutive_known_pages += 1
                    log.info(
                        "  No new jobs on page (%d/%d consecutive known pages).",
                        consecutive_known_pages, STOP_AFTER_CONSECUTIVE_KNOWN_PAGES,
                    )
                    if consecutive_known_pages >= STOP_AFTER_CONSECUTIVE_KNOWN_PAGES:
                        log.info("Caught up with previously scraped jobs. Stopping pagination.")
                        break
                else:
                    consecutive_known_pages = 0
                    log.info("  New jobs found on page: %d", new_on_this_page)
            elif new_on_this_page == 0:
                log.info("No new URLs on page (likely repeated last page). Stopping.")
                break

            # Persist discovery progress after every page
            store.write_json(NEW_URLS_CHECKPOINT_PATH, {"new_urls": new_job_urls})
            page_num += 1
    finally:
        if driver is not None:
            safe_quit(driver)

    return clean


# ============================================================
# PHASE 2: SCRAPE JOB PAGES
# ============================================================

def fetch_job_html(session, url):
    """GET with retries on network errors / 429 / 5xx. Returns HTML or None."""
    for attempt in range(1, REQUEST_MAX_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning("HTTP %s for %s (attempt %d/%d)",
                            resp.status_code, url, attempt, REQUEST_MAX_ATTEMPTS)
            else:
                log.warning("HTTP %s for %s; skipping", resp.status_code, url)
                return None
        except requests.RequestException as e:
            log.warning("Request error for %s (attempt %d/%d): %s",
                        url, attempt, REQUEST_MAX_ATTEMPTS, e)
        if attempt < REQUEST_MAX_ATTEMPTS:
            time.sleep(2 ** attempt)
    return None


def parse_job_page(html, url):
    soup = BeautifulSoup(html, "html.parser")

    # JOB ID
    apply_button = soup.select_one(".apply-btn")
    job_id = apply_button.get("data-job-id") if apply_button else None

    # JOB TITLE
    title_element = soup.select_one("h3.job-title-text")
    job_title = title_element.get_text(" ", strip=True) if title_element else None

    # COMPANY NAME
    company_element = soup.select_one(
        "a.job-meta-company, .job-company-name, .company-name, a.company-name-link"
    )
    company_name = company_element.get_text(" ", strip=True) if company_element else None

    # WORK MODE + JOB TYPE
    work_mode = None
    job_type = None
    for tag in soup.select(".job-tags .job-tag"):
        value = tag.get_text(" ", strip=True)
        value_lower = value.lower()
        if value_lower in ("on-site", "remote", "hybrid"):
            work_mode = value
        elif value_lower in (
            "full time", "part time", "contract",
            "internship", "temporary", "freelance",
        ):
            job_type = value

    # LOCATION
    country = None
    city = None
    location_element = soup.select_one(".job-meta-item span")
    if location_element:
        parts = [x.strip() for x in location_element.get_text(" ", strip=True).split(",")]
        if len(parts) >= 1:
            country = parts[0]
        if len(parts) >= 2:
            city = parts[1]

    # POSTED DATE
    date_element = soup.select_one(".job-date")
    raw_posted_date = date_element.get("data-datetime") if date_element else None
    parsed_posted_date = (
        pd.to_datetime(raw_posted_date, errors="coerce") if raw_posted_date else None
    )
    posted_date = format_datetime(parsed_posted_date) if parsed_posted_date is not None else "N/A"

    # JOB DETAILS
    job_details = {}
    for row in soup.select(".meta-data .meta"):
        spans = row.find_all("span")
        if len(spans) >= 2:
            job_details[spans[0].get_text(" ", strip=True)] = spans[1].get_text(" ", strip=True)

    experience = job_details.get("Experience")
    employment = job_details.get("Employment")
    education = job_details.get("Education")
    career_level = job_details.get("Career Level")
    salary = job_details.get("Salary")

    # SKILLS
    skills_list = [
        s.get_text(" ", strip=True)
        for s in soup.select(".job-skills .skill, .skills-list .skill-tag")
        if s.get_text(strip=True)
    ]
    skills = "; ".join(skills_list) if skills_list else None

    # JOB DESCRIPTION
    description_element = soup.select_one("#jobDescriptionBody")
    description = description_element.get_text(" ", strip=True) if description_element else None

    return {
        "job_id": job_id if job_id else "N/A",
        "source": SOURCE_NAME,
        "job_title": job_title if job_title else "N/A",
        "company_name": company_name if company_name else "N/A",
        "job_description": description if description else "N/A",
        "job_url": url if url else "N/A",
        "country": country if country else "N/A",
        "city": city if city else "N/A",
        "work_mode": work_mode if work_mode else "N/A",
        "employment_type": employment if employment else (job_type if job_type else "N/A"),
        "career_level": career_level if career_level else "N/A",
        "experience_needed": experience if experience else "N/A",
        "qualification_required": education if education else "N/A",
        "skills": skills if skills else "N/A",
        "salary": salary if salary else "N/A",
        "estimated_salary": "",
        "posted_date": posted_date if posted_date else "N/A",
        "collected_at": format_datetime(datetime.now()),
    }


def merge_into_pending(pending, pending_urls, jobs):
    """Append jobs not already queued (dedupe by URL)."""
    for job in jobs:
        url = job.get("job_url")
        if url not in pending_urls:
            pending.append(job)
            pending_urls.add(url)


def scrape_new_jobs(store, new_job_urls, all_jobs, pending, pending_urls):
    """
    Scrape each discovered URL. Every flush writes the pending queue FIRST and
    the master checkpoint SECOND: a crash between the two just means those jobs
    get re-scraped and de-duplicated, never that they are lost from the queue.
    Returns (new_jobs_this_run, completed_all).
    """
    already_scraped = known_urls_from_jobs(all_jobs)
    new_jobs_this_run = []
    unflushed = []
    failed = 0
    completed_all = True

    def flush():
        merge_into_pending(pending, pending_urls, unflushed)
        unflushed.clear()
        store.write_json(PENDING_RAW_QUEUE_PATH, pending)
        store.write_json(JOBS_CHECKPOINT_PATH, all_jobs)

    session = requests.Session()
    session.headers.update(HTTP_HEADERS)

    try:
        for i, url in enumerate(new_job_urls, start=1):
            if shutdown_requested.is_set():
                log.warning("Shutdown requested; stopping scrape.")
                completed_all = False
                break

            if url in already_scraped:
                continue

            log.info("[%d/%d] Scraping %s", i, len(new_job_urls), url)

            try:
                html = fetch_job_html(session, url)
                if html is None:
                    failed += 1
                    continue

                job = parse_job_page(html, url)
                all_jobs.append(job)
                new_jobs_this_run.append(job)
                unflushed.append(job)
                already_scraped.add(url)
                log.info("  Scraped: %s", job["job_title"])

                if len(new_jobs_this_run) % JOBS_SAVE_EVERY == 0:
                    flush()
                    log.info("  Checkpoint saved: %d total, %d new this run",
                             len(all_jobs), len(new_jobs_this_run))
            except Exception:
                failed += 1
                log.exception("Unexpected error scraping %s", url)

            time.sleep(REQUEST_DELAY_SECONDS)
    finally:
        session.close()
        if unflushed:
            flush()

    if failed:
        log.warning("%d job page(s) could not be scraped this run.", failed)
    return new_jobs_this_run, completed_all


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
    store = AzureJsonStore.from_env()

    # ---- Load state ----
    all_jobs = store.read_json(JOBS_CHECKPOINT_PATH, [])
    known_job_urls = known_urls_from_jobs(all_jobs)
    log.info("Jobs already in dataset: %d", len(all_jobs))
    log.info("Incremental mode: %s", "ON" if INCREMENTAL_MODE else "OFF (full scrape)")

    new_job_urls = store.read_json(NEW_URLS_CHECKPOINT_PATH, {}).get("new_urls", [])
    if new_job_urls:
        log.info("Resuming interrupted run: %d URL(s) already discovered.", len(new_job_urls))

    # ---- Phase 1: discovery ----
    discovery_clean = discover_new_urls(store, known_job_urls, new_job_urls)
    log.info("New job URLs to scrape this run: %d", len(new_job_urls))

    # Persist the final discovery state so an interruption in phase 2 resumes cleanly
    if new_job_urls:
        store.write_json(NEW_URLS_CHECKPOINT_PATH, {"new_urls": new_job_urls})

    # ---- Phase 2: scrape ----
    pending = store.read_json(PENDING_RAW_QUEUE_PATH, [])
    pending_urls = {j.get("job_url") for j in pending}

    new_jobs, scrape_complete = scrape_new_jobs(
        store, new_job_urls, all_jobs, pending, pending_urls
    )

    # ---- Phase 3: finalize ----
    # Only refresh the big raw file if something changed (or it doesn't exist yet).
    if new_jobs or not store.exists(TANQEEB_RAW_PATH):
        store.write_json(TANQEEB_RAW_PATH, all_jobs)
    else:
        log.info("No new jobs; raw file left untouched.")

    # Discovery checkpoint is only cleared once every discovered URL was handled.
    # (If pagination was cut short, the next run simply rediscovers from page 1.)
    if scrape_complete:
        store.delete(NEW_URLS_CHECKPOINT_PATH)

    log.info("=" * 60)
    log.info("New jobs this run:            %d", len(new_jobs))
    log.info("Total jobs in raw history:    %d", len(all_jobs))
    log.info("Jobs pending cleaning (queue): %d", len(pending))
    log.info("Elapsed: %.1f min", (time.time() - started) / 60)
    log.info("=" * 60)

    if shutdown_requested.is_set() or not scrape_complete or not discovery_clean:
        log.warning("Run finished incompletely; the next run will pick up where this left off.")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        logging.getLogger("tanqeeb_scraper").exception("Fatal error")
        sys.exit(2)
