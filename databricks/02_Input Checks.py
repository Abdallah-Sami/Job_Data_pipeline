# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

print("run Input Checks on Bronze...")

bronze_current_batch = spark.table(BRONZE_TABLE).filter((F.col("_load_date") == load_date) & (F.col("_batch_id") == batch_id))


batch_count = bronze_current_batch.count()
assert batch_count > 0, f"error: {batch_id} for date {load_date} isn't in the bronze!"

title_valid = (
    F.col("raw_job_title").isNotNull() & 
    (~F.lower(F.trim(F.col("raw_job_title"))).isin("", "n/a", "nan", "none", "null"))
)

url_valid = (F.col("raw_job_url").isNotNull()) & (F.trim(F.col("raw_job_url")) != "")

source_valid = (F.col("source").isNotNull()) & (F.trim(F.col("source")) != "")

desc_valid = (
    F.col("raw_job_description").isNotNull() & 
    (~F.lower(F.trim(F.col("raw_job_description"))).isin("", "n/a", "nan", "none", "null"))
)

collected_valid = (F.col("raw_collected_at").isNotNull()) & (F.trim(F.col("raw_collected_at")) != "")

country_str = F.lower(F.trim(F.col("raw_country")))

country_valid = (
    F.col("raw_country").isNotNull() & 
    (~country_str.isin("", "n/a", "nan", "none", "null", "egypt", "cairo", "us", "usa")) &
    (country_str.rlike("saudi|المملكة|السعودية|ksa"))
)

valid_condition = title_valid & url_valid & source_valid & desc_valid & collected_valid & country_valid

df_bronze_valid = bronze_current_batch.filter(valid_condition)
df_rejected = bronze_current_batch.filter(~valid_condition)

if df_rejected.count() > 0:
    df_rejected.write.format("delta").mode("append").saveAsTable(REJECTS_TABLE)
    print(f"{df_rejected.count()} rejected data saved in {REJECTS_TABLE}")

print(f"high quality data ready for Silver: {df_bronze_valid.count()}")