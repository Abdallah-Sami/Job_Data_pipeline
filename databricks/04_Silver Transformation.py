# Databricks notebook source
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

# MAGIC %run "./02_Input Checks"

# COMMAND ----------

# MAGIC %run "./03_City Mapping & Standardizers"

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the functions to clean and do the transformation

# COMMAND ----------

from pyspark.sql.window import Window

print("Processing Bronze to Silver...")

# clean and standardize
stg_df = df_bronze_valid \
    .withColumn("source", F.col("source")) \
    .withColumn("job_title", clean_html_spark(F.col("raw_job_title"))) \
    .withColumn("company_name", F.coalesce(clean_html_spark(F.col("raw_company_name")), F.lit("Not Specified"))) \
    .withColumn("job_description", F.coalesce(clean_html_spark(F.col("raw_job_description")), F.lit("Not Specified"))) \
    .withColumn("job_url", F.trim(F.col("raw_job_url"))) \
    .withColumn("country", F.lit("Saudi Arabia")) \
    .withColumn("city", map_city_udf(F.col("raw_city"))) \
    .withColumn("work_mode", F.when(F.lower(F.col("raw_work_mode")).contains("remote"), "Remote")
                            .when(F.lower(F.col("raw_work_mode")).contains("hybrid"), "Hybrid")
                            .when(F.lower(F.col("raw_work_mode")).contains("field"), "Field")
                            .otherwise("On-site")) \
    .withColumn("employment_type", F.when(F.lower(F.col("raw_employment_type")).contains("full"), "Full-time")
                                    .when(F.lower(F.col("raw_employment_type")).contains("part"), "Part-time")
                                    .when(F.lower(F.col("raw_employment_type")).contains("contract"), "Contract")
                                    .when(F.lower(F.col("raw_employment_type")).contains("intern"), "Internship")
                                    .when(F.lower(F.col("raw_employment_type")).contains("freelance"), "Freelance")
                                    .otherwise("Unknown")) \
    .withColumn("qualification_required", F.when(F.lower(F.col("raw_qualification")).rlike("بكالوريوس|bachelor|جامعي"), "Bachelor's Degree")
                                           .when(F.lower(F.col("raw_qualification")).rlike("ماجستير|master|دكتوراه"), "Master's Degree / PhD")
                                           .when(F.lower(F.col("raw_qualification")).rlike("دبلوم|diploma"), "Diploma")
                                           .when(F.lower(F.col("raw_qualification")).rlike("ثانوي|high school"), "High School")
                                           .otherwise("Unknown")) \
    .withColumn("experience_needed", F.coalesce(clean_html_spark(F.col("raw_experience")), F.lit("Unknown"))) \
    .withColumn("skills", F.coalesce(clean_html_spark(F.col("raw_skills")), F.lit("Not Specified"))) \
    .withColumn("salary", F.coalesce(clean_html_spark(F.col("raw_salary")), F.lit("Not Specified"))) \
    .withColumn("estimated_salary", F.lit("Not Specified")) \
    .withColumn("posted_date", parse_date_spark(F.col("raw_posted_date"))) \
    .withColumn("collected_at", F.coalesce(parse_date_spark(F.col("raw_collected_at")), F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

stg_df = stg_df.withColumn("posted_date", F.coalesce(F.col("posted_date"), F.col("collected_at")))

# COMMAND ----------

# filter non saudi jobs
non_saudi_filter = (
    F.col("job_url").contains("jobs-in-egypt") | 
    F.col("city").isin('Cairo', 'Cairo - Egypt', 'Domiat', 'Kansas', 'Missouri', 'Saudi Arabiaunited Arab Emirates - Saudi Arabia')
)
stg_df = stg_df.filter(~non_saudi_filter)

# COMMAND ----------

# generate job id
stg_df = stg_df.withColumn(
    "job_id",
    F.substring(
        F.md5(F.concat_ws("_", F.lower(F.trim(F.col("source"))), F.lower(F.trim(F.col("job_url"))), F.lower(F.trim(F.col("job_title"))))),
        1, 12
    )
)

# COMMAND ----------

# remove duplicates
w = Window.partitionBy("job_title", "company_name", "city").orderBy(F.col("collected_at").desc())
changes_df = stg_df \
    .withColumn("_rn", F.row_number().over(w)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")

# COMMAND ----------

# select columns 
final_columns = [
    'job_id', 'source', 'job_title', 'company_name', 'job_description',
    'job_url', 'country', 'city', 'work_mode', 'employment_type',
    'experience_needed', 'qualification_required', 'skills', 'salary',
    'estimated_salary', 'posted_date', 'collected_at'
]
changes_df = changes_df.select(final_columns)
print(f"{changes_df.count()} unique id ready for Silver.")