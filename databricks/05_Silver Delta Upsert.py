# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

#check the batch
batch_count = df_bronze_valid.count() if 'df_bronze_valid' in locals() else spark.table(BRONZE_TABLE).filter(
    (F.col("_load_date") == load_date) & (F.col("_batch_id") == batch_id)
).count()

if batch_count == 0:
    print("No records to process in Silver")
    dbutils.notebook.exit("Success: No records to process in Silver")

# COMMAND ----------

# MAGIC %run "./04_Silver Transformation"

# COMMAND ----------

# Create silver table
if not spark.catalog.tableExists(SILVER_TABLE):
    print(f"Creating {SILVER_TABLE}")
    changes_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
    
    # تفعيل الميزات الموصى بها في المستند
    spark.sql(f"""
        ALTER TABLE {SILVER_TABLE} 
        SET TBLPROPERTIES (
            'delta.enableDeletionVectors' = 'true',
            'delta.enableChangeDataFeed'  = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
else:
    # Do incremental update
    silver_delta = DeltaTable.forName(spark, SILVER_TABLE)
    
    (silver_delta.alias("target")
        .merge(
            changes_df.alias("source"),
            "target.job_id = source.job_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

print(f"Data is updated and merged in {SILVER_TABLE}")