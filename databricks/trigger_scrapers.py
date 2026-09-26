# Databricks notebook source
# MAGIC %md
# MAGIC # trigger_scrapers
# MAGIC Task 1 of the daily Databricks job. Starts the three Azure Container Apps Jobs
# MAGIC (Sabbar, Wuzzuf, Tanqeeb) through the Azure Management REST API and waits until
# MAGIC all of them finish. Notebooks 01 → 09 run only after this task succeeds.
# MAGIC
# MAGIC **Credentials come from the Databricks secret scope `job-pipeline`** — never paste
# MAGIC them into this notebook. See `databricks/README.md` for the one-time setup.

# COMMAND ----------

import time
import requests

SCOPE = "job-pipeline"

TENANT_ID = dbutils.secrets.get(SCOPE, "sp-tenant-id")
CLIENT_ID = dbutils.secrets.get(SCOPE, "sp-client-id")
CLIENT_SECRET = dbutils.secrets.get(SCOPE, "sp-client-secret")
SUBSCRIPTION_ID = dbutils.secrets.get(SCOPE, "azure-subscription-id")
RESOURCE_GROUP = dbutils.secrets.get(SCOPE, "azure-resource-group")

JOBS = ["sabbarcontiner", "wuzzufcontiner", "tanqeebscrape"]

BASE = (
    f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}"
    f"/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.App/jobs"
)
API = "api-version=2024-03-01"

POLL_SECONDS = 120
MAX_WAIT_HOURS = 10  # safety net: fail the task instead of waiting forever


def get_headers():
    """Fresh Azure AD token. Tokens expire after ~1 hour, and a long scrape can
    run for several hours, so a new token is requested for every poll."""
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://management.azure.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return {
        "Authorization": f"Bearer {resp.json()['access_token']}",
        "Content-Type": "application/json",
    }

# COMMAND ----------

# Start all jobs. No request body: each job keeps its own configured
# secrets and environment variables.
executions = {}
headers = get_headers()

for job_name in JOBS:
    resp = requests.post(f"{BASE}/{job_name}/start?{API}", headers=headers, timeout=60)
    resp.raise_for_status()
    executions[job_name] = resp.json()["name"]
    print(f"Started {job_name}: {executions[job_name]}")

# COMMAND ----------

# Poll until every job has finished.
deadline = time.time() + MAX_WAIT_HOURS * 3600

while True:
    time.sleep(POLL_SECONDS)
    headers = get_headers()
    all_succeeded = True

    print("\nCurrent status:")
    for job_name, exec_name in executions.items():
        resp = requests.get(f"{BASE}/{job_name}/executions/{exec_name}?{API}",
                            headers=headers, timeout=60)
        resp.raise_for_status()
        status = resp.json()["properties"]["status"]
        print(f"  {job_name}: {status}")

        if status in ("Failed", "Canceled", "Stopped"):
            raise Exception(f"{job_name} {status}. Execution: {exec_name}")
        if status != "Succeeded":
            all_succeeded = False

    if all_succeeded:
        print("\nAll scrapers finished successfully.")
        break

    if time.time() > deadline:
        raise Exception(f"Scrapers still running after {MAX_WAIT_HOURS} hours — stopping.")
