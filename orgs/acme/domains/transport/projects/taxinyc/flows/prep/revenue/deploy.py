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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Define function  for running a job

# COMMAND ----------

def run_job(*, dbutils, job_id):
    """Run job now"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    return requests.post(
        f"{api_host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"job_id": job_id},
    ).json()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create dev job

# COMMAND ----------

# Deploy jobs based on deployment.yml, in dev mode, specified by by env param
response = autojob(env="dev")

# COMMAND ----------

response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run job with python code

# COMMAND ----------

run_job(dbutils=dbutils, job_id=response['response']['job_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

# os.environ['PIPELINE_ENV'] = 'prod'
# # Deploy jobs based on deployment.yml, in dev mode
# prod_response = autojob(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod job with python

# COMMAND ----------

# run_job(dbutils=dbutils, job_id=prod_response['response']['job_id'])
