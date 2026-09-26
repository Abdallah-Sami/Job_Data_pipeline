# Databricks notebook source
# MAGIC %md
# MAGIC # Export Gold data to jobs.json for the "مسار" static website
# MAGIC Reads gold.fact_job_posting + dimensions, writes a flat JSON array to the
# MAGIC `$web` container so the Azure Static Website (index.html) can fetch it.
# MAGIC
# MAGIC Run this AFTER `08_Silver to gold load`.

# COMMAND ----------

CATALOG = "job_data_pipeline_databricks"

df = spark.sql(f"""
    SELECT
        f.job_url,
        f.job_id,
        j.job_title,
        c.company_name,
        l.city,
        s.source_name,
        f.posted_date_key
    FROM {CATALOG}.gold.fact_job_posting f
    LEFT JOIN {CATALOG}.gold.dim_job j       ON f.job_key = j.job_key
    LEFT JOIN {CATALOG}.gold.dim_company c   ON f.company_key = c.company_key
    LEFT JOIN {CATALOG}.gold.dim_location l  ON f.location_key = l.location_key
    LEFT JOIN {CATALOG}.gold.dim_source s    ON f.source_key = s.source_key
    WHERE f.job_url IS NOT NULL
    ORDER BY f.posted_date_key DESC
""")

print(f"Rows to export: {df.count()}")
display(df.limit(5))

# COMMAND ----------

import json

rows = df.toPandas().to_dict(orient="records")
json_str = json.dumps(rows, ensure_ascii=False, default=str)

WEB_PATH = "abfss://$web@jobpipelinedatalake.dfs.core.windows.net/jobs.json"

dbutils.fs.put(WEB_PATH, json_str, overwrite=True)

print(f"Exported {len(rows)} jobs to {WEB_PATH}")
print("Site: https://jobpipelinedatalake.z1.web.core.windows.net/")

