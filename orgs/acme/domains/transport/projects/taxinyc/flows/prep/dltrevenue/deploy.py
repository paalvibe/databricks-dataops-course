# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy pipelines defined by deployment.yml

# COMMAND ----------

!pip install brickops=0.3.16

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

# Deploy pipelines based on deployment.yml, in dev mode, specified by env param

response = autopipeline(env="dev")
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
# run_pipeline_by_name(dbutils=dbutils, 
#    pipeline_name=response["pipeline_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

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

# COMMAND ----------


