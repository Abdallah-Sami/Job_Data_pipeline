# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

dbutils.widgets.text("RUN_DATE", "", "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("BATCH_ID", "", "Batch ID")

param_run_date = dbutils.widgets.get("RUN_DATE").strip()
param_batch_id = dbutils.widgets.get("BATCH_ID").strip()

CATALOG = "job_data_pipeline_databricks"

BRONZE_TABLE = f"{CATALOG}.bronze.job_postings"
SILVER_TABLE = f"{CATALOG}.silver.jobs_standardized"
REJECTS_TABLE = f"{CATALOG}.rejected.job_postings"

RAW_DATA_PATH = f"/Volumes/job_data_pipeline_databricks/bronze/landing/"
IN_PROGRESS_PATH ="/Volumes/job_data_pipeline_databricks/bronze/in-process/"
ARCHIVE_PATH ="/Volumes/job_data_pipeline_databricks/bronze/archive/"


current_ts = datetime.now()

load_date = param_run_date if param_run_date else current_ts.strftime('%Y-%m-%d')
batch_id = param_batch_id if param_batch_id else current_ts.strftime('%Y%m%d_%H%M%S')

print(f"Batch ID = {batch_id}, Load Date = {load_date}")