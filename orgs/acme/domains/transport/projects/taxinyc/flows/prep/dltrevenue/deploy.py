# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy jobs defined by deployment.yml

# COMMAND ----------

!pip install pyyaml

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
from libs.dataops.deploy.autojob import autojob
from libs.dataops.job import run_job_by_name

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
db = dbname(db="dltrevenue_stage", cat=cat)
print("New db name: " + db)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy DLT pipeline

# COMMAND ----------


