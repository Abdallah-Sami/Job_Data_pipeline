# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS job_data_pipeline_databricks.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG job_data_pipeline_databricks;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_date (
# MAGIC     date_key      BIGINT NOT NULL,
# MAGIC     date          DATE NOT NULL,
# MAGIC     year          INT,
# MAGIC     month         INT,
# MAGIC     day           INT,
# MAGIC     quarter       INT,
# MAGIC     day_of_week   STRING,
# MAGIC     CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Conformed date dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_source
# MAGIC CREATE TABLE IF NOT EXISTS dim_source (
# MAGIC     source_key    BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name   STRING NOT NULL,
# MAGIC     CONSTRAINT pk_dim_source PRIMARY KEY (source_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Conformed source dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_company (
# MAGIC     company_key   BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     company_name  STRING NOT NULL,
# MAGIC     industry      STRING,
# MAGIC     CONSTRAINT pk_dim_company PRIMARY KEY (company_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Conformed company dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_location (
# MAGIC     location_key  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     country       STRING,
# MAGIC     city          STRING,
# MAGIC     CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Job location dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_job (
# MAGIC     job_key                  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     job_title                STRING,
# MAGIC     job_category              STRING,
# MAGIC     employment_type          STRING,
# MAGIC     work_mode                STRING,
# MAGIC     career_level             STRING,
# MAGIC     qualification_required   STRING,
# MAGIC     experience_needed        STRING,
# MAGIC     CONSTRAINT pk_dim_job PRIMARY KEY (job_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Job attribute-combination dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_skill (
# MAGIC     skill_key      BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     skill_name     STRING NOT NULL,
# MAGIC     skill_category STRING,
# MAGIC     CONSTRAINT pk_dim_skill PRIMARY KEY (skill_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Skill details dimension';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- job_posting_key is the fact table's own grain key (one row per posting)
# MAGIC -- job_key is now just a normal FK column pointing to dim_job (attribute combo)
# MAGIC -- job_id retained as traceable attribute
# MAGIC CREATE OR REPLACE TABLE fact_job_posting (
# MAGIC     job_posting_key      BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     job_key               BIGINT,
# MAGIC     job_id                STRING NOT NULL,
# MAGIC     unified_job_id        INT,
# MAGIC     company_key           BIGINT,
# MAGIC     location_key          BIGINT,
# MAGIC     source_key            BIGINT,
# MAGIC     posted_date_key       BIGINT,
# MAGIC     source_job_key        INT,
# MAGIC     job_url                STRING,
# MAGIC     salary                 STRING,
# MAGIC     AI_salary              STRING,
# MAGIC
# MAGIC     CONSTRAINT pk_fact_job_posting
# MAGIC         PRIMARY KEY (job_posting_key),
# MAGIC
# MAGIC     CONSTRAINT fk_fact_job
# MAGIC         FOREIGN KEY (job_key)
# MAGIC         REFERENCES dim_job (job_key),
# MAGIC
# MAGIC     CONSTRAINT fk_fact_company
# MAGIC         FOREIGN KEY (company_key)
# MAGIC         REFERENCES dim_company (company_key),
# MAGIC
# MAGIC     CONSTRAINT fk_fact_location
# MAGIC         FOREIGN KEY (location_key)
# MAGIC         REFERENCES dim_location (location_key),
# MAGIC
# MAGIC     CONSTRAINT fk_fact_source
# MAGIC         FOREIGN KEY (source_key)
# MAGIC         REFERENCES dim_source (source_key),
# MAGIC
# MAGIC     CONSTRAINT fk_fact_posted_dt
# MAGIC         FOREIGN KEY (posted_date_key)
# MAGIC         REFERENCES dim_date (date_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Fact table - grain: one row per unique job posting per source';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bridge now links via job_key (surrogate) instead of job_id
# MAGIC CREATE  or replace TABLE bridge_job_posting_skill (
# MAGIC     job_posting_key BIGINT NOT NULL,
# MAGIC     skill_key       BIGINT NOT NULL,
# MAGIC     CONSTRAINT pk_bridge_job_posting_skill PRIMARY KEY (job_posting_key, skill_key),
# MAGIC     CONSTRAINT fk_bridge_posting FOREIGN KEY (job_posting_key) REFERENCES fact_job_posting (job_posting_key),
# MAGIC     CONSTRAINT fk_bridge_skill FOREIGN KEY (skill_key) REFERENCES dim_skill (skill_key)
# MAGIC )