# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy jobs defined by deployment.yml

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

!pip install pyyaml

# COMMAND ----------

from libs.dataops.deploy.autojob import autojob
# Deploy jobs based on deployment.yml, in dev mode
response = autojob(env="dev")

# COMMAND ----------

response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run job with python code, instead of UI

# COMMAND ----------

import requests

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

run_job(dbutils=dbutils, job_id=response['response']['job_id'])

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

from libs.dataops.deploy.autojob import autojob
os.environ['PIPELINE_ENV'] = 'prod'
# Deploy jobs based on deployment.yml, in dev mode
prod_response = autojob(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod job with python

# COMMAND ----------

run_job(dbutils=dbutils, job_id=prod_response['response']['job_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Notes, scratchbook

# COMMAND ----------

# from libs.dbname import dbname
# from libs.tblname import tblname
# from libs.catname import catname_from_path
# cat = catname_from_path()
# db = dbname(db="revenue", cat=cat)
# db
