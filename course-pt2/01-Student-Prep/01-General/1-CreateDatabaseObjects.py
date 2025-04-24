# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Create a student-specific database<BR>

# COMMAND ----------

# MAGIC %md ### 0. Select a cluster: Serverless
# MAGIC
# MAGIC Press the Connect dropdown in the upper left menu, and select Serverless

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the revenue database in Databricks
# MAGIC
# MAGIC Specific for the student, git branch and commit hash

# COMMAND ----------

# MAGIC %pip install brickops=0.3.16

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Create schemas (AKA databases)

# COMMAND ----------

# MAGIC %pip install brickops==0.3.16

# COMMAND ----------

# Restart python to have access to pip modules
dbutils.library.restartPython()

# COMMAND ----------

from brickops.datamesh.naming import dbname

# COMMAND ----------

catalog = "acme_transport_taxinyc"
revenue_db = dbname(cat=catalog, db="revenue")
print("New db name: " + revenue_db)
spark.sql(f"USE catalog {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {revenue_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG acme_transport_taxinyc;
# MAGIC SHOW DATABASES;

# COMMAND ----------


