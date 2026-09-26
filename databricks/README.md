# Databricks notebooks

The daily Databricks job runs these tasks in order. Each task starts only if the previous one succeeded.

| Task | Notebook | Stage |
|---|---|---|
| 1 | `trigger_scrapers` | Start the 3 Container Apps Jobs and wait for them |
| 2 | `00_Setup & Metadata` | Shared config (catalog, paths, batch id) — `%run` by every notebook |
| 3 | `01_Bronze Delta Table` | Ingest & unify the 3 source formats into `bronze.job_postings` |
| 4 | `02_Input Checks` | Validate input; invalid rows → `rejected.job_postings` |
| 5 | `03_City Mapping & Standardizers` | Cleaning UDFs (cities, HTML, dates, job_id) |
| 6 | `04_Silver Transformation` | Clean, classify and deduplicate |
| 7 | `05_Silver Delta Upsert` | MERGE into `silver.jobs_standardized` |
| 8 | `06_Output Checks` | Quality gate — any failed assert stops the job |
| 9 | `07_data_modeling` | Create the Gold star schema tables |
| 10 | `08_Silver to gold load` | Incremental load of dimensions + `fact_job_posting`; archive raw files |
| 11 | `09_Export_Website_JSON` | Write `jobs.json` to the `$web` container for the website |


## One-time secret setup

`trigger_scrapers` reads its credentials from the secret scope `job-pipeline`:

```bash
databricks secrets create-scope job-pipeline
databricks secrets put-secret job-pipeline sp-tenant-id
databricks secrets put-secret job-pipeline sp-client-id
databricks secrets put-secret job-pipeline sp-client-secret
databricks secrets put-secret job-pipeline azure-subscription-id
databricks secrets put-secret job-pipeline azure-resource-group
```

## Keeping notebooks in sync

Link a **Databricks Git folder** to this repo (Workspace → Create → Git folder) so edits in Databricks
are committed straight to GitHub. Never commit a notebook that contains a key, secret or connection string.
