# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy pipelines defined by deployment.yml

# COMMAND ----------

# # Enable live reloading of libs, not needed now
# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

!pip install brickops==0.3.16

# COMMAND ----------

# Restart python to have access to pip modules
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Import libs

# COMMAND ----------

# Restart python to access updated packages
dbutils.library.restartPython()

# COMMAND ----------

# # # Enable live reloading of libs, not needed now
# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import requests
from brickops.dataops.deploy.autopipeline import autopipeline
from brickops.dataops.pipeline import run_pipeline_by_name, run_pipeline

# COMMAND ----------

# Name functions enables automatic env+user specific database naming
from brickops.datamesh.naming import catname_from_path
from brickops.datamesh.naming import dbname

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Unity catalog database for staging run
# MAGIC A Catalog will be created prefixed with username, branch and has commit

# COMMAND ----------

cat = catname_from_path()
db = dbname(db="dltrevenue", cat=cat)
print("New db name: " + db)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy DLT pipeline

# COMMAND ----------

# Show more output from brickops
import logging
logging.getLogger("brickops").setLevel(logging.INFO)

# COMMAND ----------

# Deploy pipelines based on deployment.yml, in dev mode, specified by env param
response = autopipeline()
response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run pipeline with python code

# COMMAND ----------

# Run the pipeline by pipeline ID
# If you get KeyError: 'pipeline_id', it could because you have recreated a pipeline, in which case
# you need to use run_pipeline_by_name()
run_pipeline(
    pipeline_id=response["response"]["pipeline_id"]
)

# COMMAND ----------

# Can be used when the pipeline created has the same name as one previously recreated,
# but note that names are no longer idempotent in Databricks
# run_pipeline_by_name(
#    pipeline_name=response["pipeline_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod
# MAGIC
# MAGIC This task will fail if the pipeline already exists as there can only be one production job.
# MAGIC The error code might be `CHANGES_UC_PIPELINE_TO_HMS_NOT_ALLOWED`.

# COMMAND ----------

# Deploy pipelines based on deployment.yml, in dev mode
# prod_response = autopipeline(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod pipeline with python

# COMMAND ----------

# run_pipeline(
#     pipeline_id=prod_response["response"]["pipeline_id"]
# )

