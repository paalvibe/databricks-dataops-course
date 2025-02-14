# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy pipelines defined by deployment.yml

# COMMAND ----------

!pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Import libs

# COMMAND ----------

# Enable live reloading of libs, not needed now
%load_ext autoreload
%autoreload 2

# COMMAND ----------

import requests
from libs.dataops.deploy.autopipeline import autopipeline
from libs.dataops.pipeline import run_pipeline_by_name, run_pipeline

# COMMAND ----------

# Name functions enables automatic env+user specific database naming
from libs.catname import catname_from_path
from libs.dbname import dbname

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

# COMMAND ----------

response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run pipeline with python code

# COMMAND ----------

response

# COMMAND ----------

response = {"pipeline_name": "acme_transport_taxinyc_prep_dev_paal_autopipeline_94c670c4"}

# COMMAND ----------

# For now we will not run pipeline by id, but name instead
# as it survives a cluster reconnect, since name is idempotent
run_pipeline(
    dbutils=dbutils, 
    pipeline_id=response["response"]["pipeline_id"]
)

# COMMAND ----------

run_pipeline_by_name(dbutils=dbutils, 
    pipeline_name=response["pipeline_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

# os.environ['PIPELINE_ENV'] = 'prod'
# # Deploy pipelines based on deployment.yml, in dev mode
# prod_response = autopipeline(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod pipeline with python

# COMMAND ----------

# run_pipeline(
#     dbutils=dbutils, 
#     pipeline_name=prod_response["response"]["pipeline_id"]
# )
