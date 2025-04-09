# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy jobs defined by deployment.yml

# COMMAND ----------

# !pip install git+https://github.com/brickops/brickops.git@86d183ee6f0f23f317922a6344fb28d42e8bd46a
%pip install git+https://github.com/brickops/brickops.git@86d183ee6f0f23f317922a6344fb28d42e8bd46a

# COMMAND ----------

# Restart python to access updated packages
dbutils.library.restartPython()

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

# MAGIC %md
# MAGIC
# MAGIC ### Create dev job

# COMMAND ----------

import logging
logging.getLogger("brickops").setLevel(logging.INFO)
# Deploy jobs based on deployment.yml, in dev mode, specified by env param
response = autojob(env="test")

# COMMAND ----------

response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run job with python code

# COMMAND ----------

run_job_by_name(dbutils=dbutils, job_name=response['job_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

# os.environ["DEPLOYMENT_ENV"] = "prod"
# Deploy jobs based on deployment.yml, in dev mode
# prod_response = autojob(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod job with python

# COMMAND ----------

# run_job_by_name(dbutils=dbutils, job_name=prod_response['job_name'])

# COMMAND ----------


