# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy jobs defined by deployment.yml

# COMMAND ----------

!pip install brickops=0.3.16

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Import libs

# COMMAND ----------

# Enable live reloading of libs, not needed now
# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import requests
from brickops.dataops.deploy.autojob import autojob
from brickops.dataops.job import run_job_by_name

# COMMAND ----------

# Name functions enables automatic env+user specific database naming
from brickops.datamesh.naming import catname_from_path
from brickops.datamesh.naming import dbname

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Unity catalog database
# MAGIC A Catalog will be created prefixed with username, branch and has commit

# COMMAND ----------

cat = catname_from_path()
db = dbname(db="dltrevenue_split", cat=cat)
print("New db name: " + db)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create dev job

# COMMAND ----------

# {
#   "name": "Example pipeline 3",
#   "storage": "dbfs:/pipeline-examples/storage-location/example3",
#   "libraries": [
#     { "notebook": { "path": "/example-notebook_1" } },
#     { "notebook": { "path": "/example-notebook_2" } },
#     { "file": { "path": "/Workspace/Users/<user-name>@databricks.com/Apply_Changes_Into/apply_changes_into.sql" } },
#     { "file": { "path": "/Workspace/Users/<user-name>@databricks.com/Apply_Changes_Into/apply_changes_into.py" } }
#   ]
# }

# COMMAND ----------

# Deploy jobs based on deployment.yml, in dev mode, specified by by env param
# response = autojob(env="dev")

# COMMAND ----------

# response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run job with python code

# COMMAND ----------

# run_job_by_name(dbutils=dbutils, job_name=response['job_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

# os.environ['PIPELINE_ENV'] = 'prod'
# Deploy jobs based on deployment.yml, in dev mode
# prod_response = autojob(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod job with python

# COMMAND ----------

# run_job_by_name(dbutils=dbutils, job_name=prod_response['job_name'])

# COMMAND ----------


