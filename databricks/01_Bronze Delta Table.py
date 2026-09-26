# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "./00_Setup & Metadata"

# COMMAND ----------

files_in_landing = dbutils.fs.ls(RAW_DATA_PATH)

sources_raw = {'wuzzuf': None, 'sabbar': None, 'tanqeeb': None}

for f_info in files_in_landing:
    if f_info.isDir():
        continue

    fname = f_info.name.lower()
    if not fname.endswith('.json'):
        continue

    if f_info.size <= 10:
        print(f"Skipping file {f_info.path} as it is empty")
        continue


    for src in sources_raw.keys():
        if fname.startswith(src):
                sources_raw[src] = f_info 


available_sources = {src: f for src, f in sources_raw.items() if f is not None}

if len(available_sources) == 0:
    print("There are no new files in landing to process")
    dbutils.notebook.exit("Success: No new files to process")

print(f"Available sources : {list(available_sources.keys())}")

#move files to in-progress
sources = {}
for src, f_info in available_sources.items():
    source_cloud_path = f_info.path
    dest_cloud_path = f"{IN_PROGRESS_PATH}{f_info.name}"
    
    dbutils.fs.mv(source_cloud_path, dest_cloud_path)

    sources[src] = dest_cloud_path.replace("dbfs:", "", 1)
    print(f"{src.upper()} was moved to In-Progress -> {dest_cloud_path}")

# COMMAND ----------


def extract_raw_records():
    records = []
    
    # Wuzzuf
    wuzzuf_path = sources.get('wuzzuf')
    if wuzzuf_path:
        with open(wuzzuf_path, "r", encoding="utf-8") as f:
            for item in json.load(f):
                attrs = item.get("job_attributes", {})
                comp = item.get("company_attributes", {})
                sal = attrs.get("salary", {})
                s_min, s_max = sal.get("min"), sal.get("max")
                s_curr = sal.get("currency", {}).get("code") if isinstance(sal.get("currency"), dict) else "SAR"
                sal_str = f"{s_min} - {s_max} {s_curr}" if s_min and s_max else (f"{s_min} {s_curr}" if s_min else None)
                
                kw_list = [k.get("name") for k in (attrs.get("keywords") or []) if isinstance(k, dict) and k.get("name")]
                loc = attrs.get("location") or {}
                city = loc.get("city", {}).get("name") if isinstance(loc.get("city"), dict) else None
                wp = attrs.get("workplaceArrangement") or {}
                work_mode = wp.get("translations", {}).get("displayedName", {}).get("en") or wp.get("displayedName")
                wt = attrs.get("workTypes") or []
                emp_type = wt[0].get("name") if wt and isinstance(wt[0], dict) else None
                exp = attrs.get("workExperienceYears") or {}
                exp_str = f"{exp.get('min')} - {exp.get('max')} years" if exp.get("min") or exp.get("max") else None
                edu = attrs.get("candidatePreferences", {}).get("educationLevel") or {}
                
                records.append({
                    "source": "Wuzzuf", "raw_job_id": str(item.get("job_id", "")),
                    "raw_job_title": attrs.get("title"), "raw_company_name": comp.get("name"),
                    "raw_job_description": attrs.get("description"), "raw_job_url": item.get("job_url"),
                    "raw_country": "Saudi Arabia", "raw_city": city, "raw_work_mode": work_mode,
                    "raw_employment_type": emp_type, "raw_experience": exp_str,
                    "raw_qualification": edu.get("name") if isinstance(edu, dict) else None,
                    "raw_skills": "; ".join(kw_list) if kw_list else None, "raw_salary": sal_str,
                    "raw_estimated_salary": None, "raw_posted_date": attrs.get("postedAt"),
                    "raw_collected_at": item.get("scraped_at")
                })
                
    # Sabbar
    sabbar_path = sources.get('sabbar')
    if sabbar_path:
        with open(sabbar_path, "r", encoding="utf-8") as f:
            for item in json.load(f):
                s_min, s_max = item.get("salary"), item.get("max_salary")
                curr = item.get("currency") or "SAR"
                sal_str = f"{s_min} - {s_max} {curr}" if s_min and s_max else (f"{s_min} {curr}" if s_min else None)
                records.append({
                    "source": "Sabbar", "raw_job_id": str(item.get("id", "")),
                    "raw_job_title": item.get("title"), "raw_company_name": item.get("company"),
                    "raw_job_description": item.get("description"), "raw_job_url": item.get("url"),
                    "raw_country": item.get("country") or "Saudi Arabia", "raw_city": item.get("city"),
                    "raw_work_mode": item.get("workplace_type"), "raw_employment_type": item.get("contract_type"),
                    "raw_experience": item.get("experience_years"), "raw_qualification": None,
                    "raw_skills": None, "raw_salary": sal_str, "raw_estimated_salary": None,
                    "raw_posted_date": item.get("created_date"), "raw_collected_at": item.get("collected_at")
                })

    # Tanqeeb
    tanqeeb_path = sources.get('tanqeeb')
    if tanqeeb_path:
        with open(tanqeeb_path, "r", encoding="utf-8") as f:
            for item in json.load(f):
                loc = item.get("location") or ""
                city = item.get("city") or (loc.split(",")[-1].strip() if "," in loc else loc)
                records.append({
                    "source": "Tanqeeb", "raw_job_id": str(item.get("id") or item.get("job_id") or ""),
                    "raw_job_title": item.get("title") or item.get("job_title"),
                    "raw_company_name": item.get("company") or item.get("company_name"),
                    "raw_job_description": item.get("description") or item.get("job_description"),
                    "raw_job_url": item.get("url") or item.get("job_url"),
                    "raw_country": item.get("country") or "Saudi Arabia", "raw_city": city,
                    "raw_work_mode": item.get("work_mode"), "raw_employment_type": item.get("job_type") or item.get("employment_type"),
                    "raw_experience": item.get("experience") or item.get("experience_needed"),
                    "raw_qualification": item.get("education") or item.get("qualification_required"),
                    "raw_skills": item.get("skills"), "raw_salary": item.get("salary"),
                    "raw_estimated_salary": item.get("estimated_salary"), "raw_posted_date": item.get("posted_date"),
                    "raw_collected_at": item.get("collected_at")
                })
    return records

# COMMAND ----------

raw_list = extract_raw_records()

bronze_schema = StructType([
    StructField("source", StringType(), True),
    StructField("raw_job_id", StringType(), True),
    StructField("raw_job_title", StringType(), True),
    StructField("raw_company_name", StringType(), True),
    StructField("raw_job_description", StringType(), True),
    StructField("raw_job_url", StringType(), True),
    StructField("raw_country", StringType(), True),
    StructField("raw_city", StringType(), True),
    StructField("raw_work_mode", StringType(), True),
    StructField("raw_employment_type", StringType(), True),
    StructField("raw_experience", StringType(), True),
    StructField("raw_qualification", StringType(), True),
    StructField("raw_skills", StringType(), True),
    StructField("raw_salary", StringType(), True),
    StructField("raw_estimated_salary", StringType(), True),
    StructField("raw_posted_date", StringType(), True),
    StructField("raw_collected_at", StringType(), True)
])

df_raw = spark.createDataFrame(raw_list, schema=bronze_schema) \
    .withColumn("_ingest_ts", F.current_timestamp()) \
    .withColumn("_load_date", F.lit(load_date).cast("date")) \
    .withColumn("_batch_id", F.lit(batch_id))

(df_raw.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_load_date = '{load_date}' AND _batch_id = '{batch_id}'")
    .partitionBy("_load_date")
    .saveAsTable(BRONZE_TABLE))

print(f"Batch saved to bronze Delta: {BRONZE_TABLE}")
display(spark.table(BRONZE_TABLE).filter(F.col("_batch_id") == batch_id).limit(3))