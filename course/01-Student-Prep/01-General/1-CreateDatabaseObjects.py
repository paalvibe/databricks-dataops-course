# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Create a student-specific database<BR>

# COMMAND ----------

# MAGIC %md ### 0. Select Serverless as compute
# MAGIC
# MAGIC Press the Connect dropdown in the upper left menu, and select Serverless
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the taxi_db database in Databricks
# MAGIC
# MAGIC Specific for the student

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Create schemas (AKA databases)

# COMMAND ----------

# %pip install brickops==0.3.12
# %pip install git+https://github.com/brickops/brickops.git@feature/file-cfg
%pip install git+https://github.com/brickops/brickops.git@86d183ee6f0f23f317922a6344fb28d42e8bd46a

# COMMAND ----------

# Restart python to access updated packages
dbutils.library.restartPython()

# COMMAND ----------

from brickops.datamesh.naming import dbname

# COMMAND ----------

# from brickops.databricks.context import current_env, get_context
# db_context = get_context()
# db_context

# COMMAND ----------

import logging
logging.getLogger("brickops").setLevel(logging.INFO)
catalog = "transport"
revenue_db = dbname(cat=catalog, db="revenue")
revenue_db

# COMMAND ----------

catalog = "transport"
revenue_db = dbname(cat=catalog, db="revenue")
print("New db name: " + revenue_db)
spark.sql(f"USE catalog {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {revenue_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG transport;
# MAGIC SHOW DATABASES;

# COMMAND ----------


