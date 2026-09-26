# Databricks notebook source
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from silver.jobs_standardized
# MAGIC

# COMMAND ----------

# %sql
# SELECT *
# FROM silver.jobs_standardized
# where source ILIKE'%Tan%'
# limit 1


# COMMAND ----------

# %sql
# SELECT
#     job_id,
#     COUNT(*) AS count
# FROM silver.jobs_standardized
# GROUP BY job_id
# HAVING COUNT(*) > 1
# ORDER BY count DESC;

# COMMAND ----------

# %sql
# select *
# from silver.jobs_standardized
# limit 1


# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "job_data_pipeline_databricks"
GOLD_SCHEMA = "gold"
SILVER_TABLE = f"{CATALOG}.silver.jobs_standardized"
PIPELINE_NAME = "silver_to_gold_job_postings"


def tbl(name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"


# ------------------------------------------------------------------
# 0. Watermark control table - tracks the last collected_at successfully loaded
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl('etl_watermark')} (
    pipeline_name    STRING NOT NULL,
    last_loaded_at   TIMESTAMP,
    CONSTRAINT pk_etl_watermark PRIMARY KEY (pipeline_name)
)
USING DELTA
COMMENT 'Tracks incremental load progress per pipeline'
""")

watermark_row = (
    spark.table(tbl("etl_watermark"))
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .select("last_loaded_at")
    .collect()
)
last_loaded_at = watermark_row[0]["last_loaded_at"] if watermark_row else None
print(f"Last watermark for '{PIPELINE_NAME}': {last_loaded_at}")

# ------------------------------------------------------------------
# 1. Read only new/changed rows from Silver since the last watermark
# ------------------------------------------------------------------
silver_df = spark.table(SILVER_TABLE)

if last_loaded_at is not None:
    changes_df = silver_df.filter(F.col("collected_at") > F.lit(last_loaded_at))
else:
    changes_df = silver_df  # first run: load everything

new_row_count = changes_df.count()
print(f"{new_row_count} new/changed rows found in Silver since last watermark.")

if new_row_count == 0:
    print("No new data since last run. Skipping load.")
else:
    # ------------------------------------------------------------------
    # 2. Prepare fields, defaulting to 'N/A' for anything Silver doesn't supply yet
    # ------------------------------------------------------------------
    def col_or_default(df, col_name, default="N/A"):
        """Return the column if it exists in df, else a literal default column."""
        if col_name in df.columns:
            return F.coalesce(F.col(col_name), F.lit(default))
        return F.lit(default)

    prepared_df = (
        changes_df
        .withColumn("job_category", col_or_default(changes_df, "job_category"))
        .withColumn("career_level", col_or_default(changes_df, "career_level"))
        .withColumn("experience_needed", F.coalesce(F.col("experience_needed"), F.lit("N/A")))
        .withColumn(
            "unified_job_id",
            F.col("unified_job_id").cast("int") if "unified_job_id" in changes_df.columns
            else F.lit(None).cast("int")
        )
        .withColumn("salary", F.col("salary").cast("string"))
        .withColumn("estimated_salary", F.col("estimated_salary").cast("string"))
    )

    print("prepared_df columns:", prepared_df.columns)

    # ------------------------------------------------------------------
    # 3. DIM_SOURCE
    # ------------------------------------------------------------------
    dim_source_updates = (
        prepared_df.select(F.col("source").alias("source_name"))
        .filter(F.col("source_name").isNotNull())
        .distinct()
    )

    DeltaTable.forName(spark, tbl("dim_source")).alias("t").merge(
        dim_source_updates.alias("s"), "t.source_name = s.source_name"
    ).whenNotMatchedInsert(values={"source_name": "s.source_name"}).execute()

    dim_source_df = spark.table(tbl("dim_source"))  # has source_key

    # ------------------------------------------------------------------
    # 4. DIM_COMPANY
    # ------------------------------------------------------------------
    dim_company_updates = (
        prepared_df.select("company_name")
        .filter(F.col("company_name").isNotNull())
        .distinct()
    )

    DeltaTable.forName(spark, tbl("dim_company")).alias("t").merge(
        dim_company_updates.alias("s"), "t.company_name = s.company_name"
    ).whenNotMatchedInsert(values={"company_name": "s.company_name"}).execute()

    dim_company_df = spark.table(tbl("dim_company"))  # has company_key

    # ------------------------------------------------------------------
    # 5. DIM_LOCATION  (region not in Silver, stays NULL)
    # ------------------------------------------------------------------
    dim_location_updates = prepared_df.select("country", "city").distinct()

    DeltaTable.forName(spark, tbl("dim_location")).alias("t").merge(
        dim_location_updates.alias("s"),
        "t.country <=> s.country AND t.city <=> s.city",
    ).whenNotMatchedInsert(
        values={"country": "s.country", "city": "s.city"}
    ).execute()

    dim_location_df = spark.table(tbl("dim_location"))  # has location_key

    # ------------------------------------------------------------------
    # 6. DIM_JOB  (attribute-combo dimension; surrogate key = job_key)
    # ------------------------------------------------------------------
    dim_job_updates = prepared_df.select(
        "job_title",
        "job_category",
        "employment_type",
        "work_mode",
        "career_level",
        "qualification_required",
        "experience_needed",
    ).distinct()

    DeltaTable.forName(spark, tbl("dim_job")).alias("t").merge(
        dim_job_updates.alias("s"),
        """
        t.job_title <=> s.job_title
        AND t.job_category <=> s.job_category
        AND t.employment_type <=> s.employment_type
        AND t.work_mode <=> s.work_mode
        AND t.career_level <=> s.career_level
        AND t.qualification_required <=> s.qualification_required
        AND t.experience_needed <=> s.experience_needed
        """,
    ).whenNotMatchedInsert(
        values={
            "job_title": "s.job_title",
            "job_category": "s.job_category",
            "employment_type": "s.employment_type",
            "work_mode": "s.work_mode",
            "career_level": "s.career_level",
            "qualification_required": "s.qualification_required",
            "experience_needed": "s.experience_needed",
        }
    ).execute()

    dim_job_df = spark.table(tbl("dim_job"))  # has job_key (identity)

    # ------------------------------------------------------------------
    # 7. DIM_DATE  (built from posted_date)
    # ------------------------------------------------------------------
    new_dates_df = (
        prepared_df.select(F.col("posted_date").cast("date").alias("date"))
        .filter(F.col("date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
    )

    DeltaTable.forName(spark, tbl("dim_date")).alias("t").merge(
        new_dates_df.alias("s"), "t.date_key = s.date_key"
    ).whenNotMatchedInsertAll().execute()

    # ------------------------------------------------------------------
    # 8. Build fact rows with surrogate keys resolved via joins
    # ------------------------------------------------------------------
    fact_prepared_df = (
        prepared_df
        .join(dim_source_df, prepared_df.source == dim_source_df.source_name, "left")
        .join(dim_company_df, "company_name", "left")
        .join(dim_location_df, ["country", "city"], "left")
        .join(
            dim_job_df,
            (prepared_df.job_title == dim_job_df.job_title)
            & (prepared_df.job_category.eqNullSafe(dim_job_df.job_category))
            & (prepared_df.employment_type == dim_job_df.employment_type)
            & (prepared_df.work_mode == dim_job_df.work_mode)
            & (prepared_df.career_level.eqNullSafe(dim_job_df.career_level))
            & (prepared_df.qualification_required == dim_job_df.qualification_required)
            & (prepared_df.experience_needed.eqNullSafe(dim_job_df.experience_needed)),
            "left",
        )
        .withColumn(
            "posted_date_key",
            F.date_format(F.col("posted_date").cast("date"), "yyyyMMdd").cast("int"),
        )
        .select(
            dim_job_df.job_key,
            dim_company_df.company_key,
            dim_location_df.location_key,
            dim_source_df.source_key,
            F.col("posted_date_key"),
            prepared_df.job_id,
            prepared_df.unified_job_id,
            prepared_df.job_url,
            prepared_df.salary,
            prepared_df.estimated_salary,
            prepared_df.skills,
        )
    )

    print("fact_prepared_df columns:", fact_prepared_df.columns)

    # ------------------------------------------------------------------
    # 9. Insert only postings not already in the fact table
    #    (dedup key = source_key + job_id)
    # ------------------------------------------------------------------
    existing_fact_df = spark.table(tbl("fact_job_posting")).select(
        "source_key", "job_id"
    )

    new_fact_df = fact_prepared_df.join(
        existing_fact_df, on=["source_key", "job_id"], how="left_anti"
    )

    # NOTE: source_job_id is intentionally left out of this write.
    # It is nullable in fact_job_posting, and Silver does not currently
    # carry a native/original job ID from the source site, so Delta
    # will just fill it with NULL for every row. Add it back here
    # (with a real source value) once Silver captures it upstream.
    fact_write_df = (
        new_fact_df
        .withColumnRenamed("estimated_salary", "AI_salary")
        .select(
            "job_id",
            "job_key",
            "unified_job_id",
            "company_key",
            "location_key",
            "source_key",
            "posted_date_key",
            "job_url",
            "salary",
            "AI_salary",
        )
    )

    print("fact_write_df columns being written to fact_job_posting:", fact_write_df.columns)

    (
        fact_write_df
        .write.format("delta")
        .mode("append")
        .saveAsTable(tbl("fact_job_posting"))
    )

    print(f"{new_fact_df.count()} new postings inserted into fact_job_posting.")

    # ------------------------------------------------------------------
    # 10. DIM_SKILL + BRIDGE  (skills assumed comma-separated string)
    # ------------------------------------------------------------------
    skills_exploded_df = (
        new_fact_df.select("source_key", "job_id", "skills")
        .withColumn("skill_name", F.explode(F.split(F.col("skills"), r"\s*,\s*")))
        .withColumn("skill_name", F.trim(F.col("skill_name")))
        .filter((F.col("skill_name").isNotNull()) & (F.col("skill_name") != ""))
    )

    dim_skill_updates = skills_exploded_df.select("skill_name").distinct()

    DeltaTable.forName(spark, tbl("dim_skill")).alias("t").merge(
        dim_skill_updates.alias("s"), "t.skill_name = s.skill_name"
    ).whenNotMatchedInsert(values={"skill_name": "s.skill_name"}).execute()

    dim_skill_df = spark.table(tbl("dim_skill"))  # has skill_key

    fact_ids_df = spark.table(tbl("fact_job_posting")).select(
        "job_posting_key", "job_id", "source_key"
    )

    bridge_rows_df = (
        skills_exploded_df.join(fact_ids_df, ["source_key", "job_id"], "inner")
        .join(dim_skill_df, "skill_name", "inner")
        .select("job_posting_key", "skill_key")
        .distinct()
    )

    (
        bridge_rows_df.write.format("delta")
        .mode("append")
        .saveAsTable(tbl("bridge_job_posting_skill"))
    )

    print(f"{bridge_rows_df.count()} skill links inserted into bridge_job_posting_skill.")

    # ------------------------------------------------------------------
    # 11. Advance the watermark to the max collected_at processed this run
    # ------------------------------------------------------------------
    new_watermark = changes_df.agg(F.max("collected_at")).collect()[0][0]

    if new_watermark is not None:
        watermark_update_df = spark.createDataFrame(
            [(PIPELINE_NAME, new_watermark)], ["pipeline_name", "last_loaded_at"]
        )

        DeltaTable.forName(spark, tbl("etl_watermark")).alias("t").merge(
            watermark_update_df.alias("s"), "t.pipeline_name = s.pipeline_name"
        ).whenMatchedUpdate(
            set={"last_loaded_at": "s.last_loaded_at"}
        ).whenNotMatchedInsert(
            values={"pipeline_name": "s.pipeline_name", "last_loaded_at": "s.last_loaded_at"}
        ).execute()

        print(f"Watermark advanced to {new_watermark}")

# COMMAND ----------

# %sql
# DELETE FROM job_data_pipeline_databricks.gold.fact_job_posting;
# DELETE FROM job_data_pipeline_databricks.gold.bridge_job_posting_skill;
# DELETE FROM job_data_pipeline_databricks.gold.dim_job;
# DELETE FROM job_data_pipeline_databricks.gold.dim_company;
# DELETE FROM job_data_pipeline_databricks.gold.dim_location;
# DELETE FROM job_data_pipeline_databricks.gold.dim_source;
# DELETE FROM job_data_pipeline_databricks.gold.dim_date;
# DELETE FROM job_data_pipeline_databricks.gold.dim_date;

# DELETE FROM job_data_pipeline_databricks.gold.etl_watermark
# WHERE pipeline_name = 'silver_to_gold_job_postings';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from gold.fact_job_posting

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.fact_job_posting
# MAGIC limit 10 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# %sql
# select *
# from gold.dim_job
# limit 1
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from gold.fact_job_posting
# MAGIC limit 1

# COMMAND ----------

#move row data to archive
in_progress_files = dbutils.fs.ls(IN_PROGRESS_PATH)

archived_count = 0
for f_info in in_progress_files:
    if f_info.name.endswith(".json"):
        archived_target = f"{ARCHIVE_PATH}{batch_id}_{f_info.name}"
        dbutils.fs.mv(f_info.path, archived_target)
        print(f"file moved to archive : {f_info.name} -> {archived_target}")
        archived_count += 1

print(f"\n{archived_count} files archived successfully !")