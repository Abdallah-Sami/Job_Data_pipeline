"""
Sabbar Scraper - Cloud (Azure Data Lake) - Daily Incremental
============================================================
Runs inside a container (ADF schedules it). Scrapes ONLY new jobs and writes
to Azure Data Lake using the Medallion architecture.

Data Lake layout:
  Container "landing":
    sabbar_<RUN_DATE>.json                         - jobs scraped in this run

  Container "checkpoints":
    sabbar/checkpoint.json                         - list of every URL ever scraped
    sabbar/sabbar_failed_<RUN_DATE>.json           - URLs that failed this run

Every job carries a `collected_at` timestamp of when the scrape happened.

Environment variables (all set by ADF at runtime):
  AZURE_STORAGE_CONNECTION - storage account connection string
  RUN_DATE                 - date used in filenames, e.g. "2026-09-21"
                             (falls back to today's UTC date if not set)
"""

import asyncio
import json
import logging
import os
import random
import sys
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AZURE_CONN = os.environ["AZURE_STORAGE_CONNECTION"]
RUN_DATE = os.environ.get("RUN_DATE") or date.today().isoformat()

CONTAINER_LANDING = "landing"
CONTAINER_CHECKPOINTS = "checkpoints"

CHECKPOINT_BLOB = "sabbar/checkpoint.json"
DAILY_BLOB = f"sabbar_{RUN_DATE}.json"
FAILED_BLOB = f"sabbar/sabbar_failed_{RUN_DATE}.json"

SITEMAP_URL = "https://sabbar.com/en/jobs/sitemaps/job-details.xml"

LIMIT = None       # None = all
CONCURRENCY = 4
MIN_DELAY = 1
MAX_DELAY = 3
RETRIES = 3
SAVE_EVERY = 200
BATCH_SIZE = 400

LAUNCH_ARGS = [
    "--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox",
    "--disable-extensions", "--disable-background-networking",
]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Proxy (Webshare) — used for both the sitemap request (requests) and
# Playwright's browser (job detail pages), to route around IP-based blocks.
PROXY_ENABLED = os.environ.get("PROXY_ENABLED", "false").lower() == "true"
PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = os.environ.get("PROXY_PORT", "")
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
REQUESTS_PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_ENABLED else None
PLAYWRIGHT_PROXY = (
    {"server": f"http://{PROXY_HOST}:{PROXY_PORT}", "username": PROXY_USER, "password": PROXY_PASS}
    if PROXY_ENABLED else None
)

# ---------------------------------------------------------------------------
# Logging (structured for Container App / Log Analytics)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("sabbar")

_blob_service = BlobServiceClient.from_connection_string(AZURE_CONN)


# ---------------------------------------------------------------------------
# Data Lake read / write
# ---------------------------------------------------------------------------
def read_json_blob(container, blob_name):
    """Read a JSON blob; return None if it doesn't exist."""
    blob = _blob_service.get_blob_client(container, blob_name)
    if not blob.exists():
        return None
    data = blob.download_blob().readall()
    return json.loads(data)


def write_json_blob(container, blob_name, obj):
    blob = _blob_service.get_blob_client(container, blob_name)
    blob.upload_blob(json.dumps(obj, ensure_ascii=False, indent=2), overwrite=True)


def load_checkpoint():
    """Return a set of every URL already scraped."""
    data = read_json_blob(CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB)
    if data is None:
        return set()
    if not isinstance(data, list):
        raise TypeError(
            f"Checkpoint must be a list of URLs, got {type(data).__name__}. "
            f"Did you run migrate_sabbar_checkpoint.py?"
        )
    return set(data)


def save_checkpoint(url_set):
    """Persist the URL set as a sorted list."""
    write_json_blob(CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB, sorted(url_set))


# ---------------------------------------------------------------------------
# Stage 1 - collect URLs from the sitemap
# ---------------------------------------------------------------------------
def collect_urls():
    res = requests.get(
        SITEMAP_URL, headers={"User-Agent": "Mozilla/5.0"},
        proxies=REQUESTS_PROXIES, timeout=30,
    )
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "xml")
    urls = [loc.text for loc in soup.find_all("loc")]
    log.info("Stage 1: found %d job URLs in sitemap", len(urls))
    return urls[:LIMIT] if LIMIT else urls


# ---------------------------------------------------------------------------
# Stage 2 - extraction
# ---------------------------------------------------------------------------
def extract_job_details(html):
    marker = '\\"jobPositionValue\\"'
    pos = html.find(marker)
    if pos == -1:
        return None
    start = html.rfind('{\\"id\\":', 0, pos)
    if start == -1:
        start = html.rfind("{", 0, pos)
    depth, i, in_string = 0, start, False
    while i < len(html):
        if html[i] == "\\" and i + 1 < len(html) and html[i + 1] == '"':
            in_string = not in_string
            i += 2
            continue
        ch = html[i]
        if not in_string:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    raw = html[start:i + 1]
                    clean = raw.replace('\\"', '"').replace("\\\\", "\\")
                    try:
                        return json.loads(clean)
                    except Exception:
                        return None
        i += 1
    return None


def pick_fields(details, url, description, run_stamp):
    return {
        "url": url, "id": details.get("id"),
        "title": details.get("jobPositionValue"),
        "company": (details.get("partnerName") or "").strip(),
        "industries": ", ".join(details.get("industries") or []),
        "salary": details.get("salary"), "max_salary": details.get("maxSalary"),
        "currency": details.get("currency"),
        "salary_frequency": details.get("salaryFrequency"),
        "contract_type": details.get("contractType"),
        "workplace_type": details.get("workplaceType"),
        "experience_years": details.get("experienceYears"),
        "requires_experience": details.get("requiresExperience"),
        "requires_english": details.get("requiresEnglish"),
        "english_proficiency": details.get("englishProficiency"),
        "requires_gosi": details.get("requiresGosi"),
        "requires_health_card": details.get("requiresHealthCard"),
        "gender": details.get("gender"),
        "saudis_only": details.get("saudisOnly"),
        "females_only_env": details.get("isEnvironmentForFemalesOnly"),
        "city": details.get("cityValue"), "district": details.get("district"),
        "address": details.get("address"), "country": details.get("countryValue"),
        "postal_code": details.get("postalCode"),
        "created_date": details.get("createdDate"),
        "job_status": details.get("jobStatus"), "is_new": details.get("isNew"),
        "source": details.get("source"), "description": description,
        "collected_at": run_stamp,
    }


async def scrape_job(page, url, run_stamp):
    """Return (job_dict_or_None, error_reason_or_None)."""
    last_error = "unknown"
    for attempt in range(RETRIES):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            description = None
            try:
                await page.wait_for_selector("div.desc", timeout=15000)
                description = (await page.inner_text("div.desc")).strip()
            except Exception:
                description = None
            html = await page.content()
            details = extract_job_details(html)
            if not details:
                return None, "no_details_extracted"
            return pick_fields(details, url, description, run_stamp), None
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)[:100]}"
            if attempt < RETRIES - 1:
                await asyncio.sleep(2)
                continue
            return None, last_error


async def scrape_batch(batch, results, failed_records, known_urls, total, run_stamp):
    queue = asyncio.Queue()
    for u in batch:
        queue.put_nowait(u)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=LAUNCH_ARGS, proxy=PLAYWRIGHT_PROXY,
        )
        context = await browser.new_context(user_agent=USER_AGENT)

        async def worker():
            page = await context.new_page()
            while not queue.empty():
                url = await queue.get()
                job, error = await scrape_job(page, url, run_stamp)
                if job:
                    results[url] = job
                    known_urls.add(url)
                elif error == "no_details_extracted":
                    # Page loaded but the job posting is gone/removed (site
                    # returns a Next.js error shell instead of a 404).
                    # Treat as permanently done so it isn't retried forever.
                    known_urls.add(url)
                    failed_records.append({
                        "url": url,
                        "attempted_at": run_stamp,
                        "reason": "gone_no_details",
                    })
                else:
                    failed_records.append({
                        "url": url,
                        "attempted_at": run_stamp,
                        "reason": error,
                    })
                if (len(results) + len(failed_records)) % SAVE_EVERY == 0:
                    save_checkpoint(known_urls)
                    log.info(
                        "[%d ok / %d failed / %d total] checkpoint saved",
                        len(results), len(failed_records), total,
                    )
                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            await page.close()

        await asyncio.gather(*[worker() for _ in range(CONCURRENCY)])
        await browser.close()
    save_checkpoint(known_urls)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def run():
    run_stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info("Sabbar scraper starting")
    log.info("  RUN_DATE:         %s", RUN_DATE)
    log.info("  Proxy enabled:    %s (%s:%s)", PROXY_ENABLED, PROXY_HOST if PROXY_ENABLED else "-", PROXY_PORT if PROXY_ENABLED else "-")
    log.info("  Landing target:   %s/%s", CONTAINER_LANDING, DAILY_BLOB)
    log.info("  Failed target:    %s/%s", CONTAINER_CHECKPOINTS, FAILED_BLOB)
    log.info("  Checkpoint:       %s/%s", CONTAINER_CHECKPOINTS, CHECKPOINT_BLOB)
    log.info("=" * 60)

    urls = collect_urls()
    known_urls = load_checkpoint()
    new_links = [u for u in urls if u not in known_urls]

    log.info(
        "Sitemap: %d | already scraped: %d | new to scrape: %d",
        len(urls), len(known_urls), len(new_links),
    )

    MAX_JOBS_PER_RUN = int(os.environ.get("MAX_JOBS_PER_RUN", "900")) or None
    if MAX_JOBS_PER_RUN and len(new_links) > MAX_JOBS_PER_RUN:
        log.info(
            "Capping this run to %d jobs (out of %d new) via MAX_JOBS_PER_RUN. "
            "Remaining will be picked up on the next run.",
            MAX_JOBS_PER_RUN, len(new_links),
        )
        new_links = new_links[:MAX_JOBS_PER_RUN]

    if not new_links:
        log.info("No new jobs today. Writing empty bronze file for downstream consistency.")
        write_json_blob(CONTAINER_LANDING, DAILY_BLOB, [])
        return

    results = {}                # {url: job_dict} for successful scrapes only
    failed_records = []         # list of {url, attempted_at, reason}
    total = len(new_links)

    for i in range(0, len(new_links), BATCH_SIZE):
        batch = new_links[i:i + BATCH_SIZE]
        for attempt in range(3):
            try:
                await scrape_batch(batch, results, failed_records, known_urls, total, run_stamp)
                break
            except Exception as e:
                log.warning("Batch error: %r; checkpoint saved, retrying", e)
                save_checkpoint(known_urls)
                batch = [u for u in batch if u not in known_urls]
                if not batch:
                    break

    # Final writes
    new_jobs = list(results.values())
    write_json_blob(CONTAINER_LANDING, DAILY_BLOB, new_jobs)
    log.info("Wrote %d jobs to %s/%s", len(new_jobs), CONTAINER_LANDING, DAILY_BLOB)

    if failed_records:
        write_json_blob(CONTAINER_CHECKPOINTS, FAILED_BLOB, failed_records)
        log.info(
            "Wrote %d failed URLs to %s/%s (will be retried on next run)",
            len(failed_records), CONTAINER_CHECKPOINTS, FAILED_BLOB,
        )

    log.info("=" * 60)
    log.info("Sabbar scraper finished")
    log.info("  New jobs scraped:    %d", len(new_jobs))
    log.info("  Failed URLs:         %d", len(failed_records))
    log.info("  Checkpoint total:    %d URLs", len(known_urls))
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        log.exception("Fatal error, exiting non-zero: %s", e)
        sys.exit(1)
