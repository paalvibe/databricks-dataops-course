# Databricks notebook source
from libs.age import gen_age

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example notebook
# MAGIC #
# MAGIC # In real life this notebook should aggregate trip data.
# MAGIC # For now it does nothing.

# COMMAND ----------

df = table("nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC % TODO aggregate trip data in df

# COMMAND ----------

# MAGIC %md
# MAGIC % TODO write aggregated data to output dataset
