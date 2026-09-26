# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

print("run output check for silver table ...")

silver_table_df = spark.table(SILVER_TABLE)
total_rows = silver_table_df.count()

# COMMAND ----------

# check if table is null
assert total_rows > 0, "error: silver table is empty"
print(f"total rows in silver: {total_rows}")

# COMMAND ----------

# check job id uniqueness
unique_ids = silver_table_df.select("job_id").distinct().count()
assert unique_ids == total_rows, f"error: silver table has {total_rows - unique_ids} ununiqe job_id!"
print(f" silver table has {unique_ids} unique ids.")

# COMMAND ----------

# check duplication
business_keys = ["job_title", "company_name", "city", "posted_date"]
business_dup_count = silver_table_df.groupBy(business_keys).count().filter("count > 1").count()

if business_dup_count > 0:
    print(f"⚠️ Found {business_dup_count} duplicate groups. Deduplicating silver table...")
    
    from pyspark.sql.window import Window
    w_dedup = Window.partitionBy(business_keys).orderBy(F.col("collected_at").desc())
    
    deduped_silver_df = silver_table_df \
        .withColumn("_rn", F.row_number().over(w_dedup)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")
    
    deduped_silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
    
    silver_table_df = spark.table(SILVER_TABLE)
    print(f"✓ Removed duplicates successfully. Total rows now: {silver_table_df.count()}")
else:
    print("✓ There is no duplicates")

# COMMAND ----------

# check unnullable columns
mandatory_cols = ['job_id', 'source', 'job_title', 'job_url', 'country', 'job_description', 'collected_at']
for col_name in mandatory_cols:
    nulls = silver_table_df.filter(F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")).count()
    assert nulls == 0, f"error: silver table has {nulls} Nulls in column [{col_name}]!"
    print(f"column [{col_name}] is OK (0 Nulls).")

# COMMAND ----------

# check country
non_saudi_cnt = silver_table_df.filter(F.col("country") != "Saudi Arabia").count()
assert non_saudi_cnt == 0, f"error: there is {non_saudi_cnt} records not from Saudi Arabia"
print("all records are from Saudi Arabia")

# COMMAND ----------

history_info = DeltaTable.forName(spark, SILVER_TABLE).history(1).select("version", "operationMetrics").collect()[0]
op_metrics = history_info["operationMetrics"] or {}
print(f"\n📈 Delta Metrics: Rows Inserted = {op_metrics.get('numTargetRowsInserted', 'N/A')}, Rows Updated = {op_metrics.get('numTargetRowsUpdated', 'N/A')}")

print("\n silver table is ready for gold!")
display(silver_table_df.limit(5))