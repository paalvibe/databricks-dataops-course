# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the borough_population table
# MAGIC
# MAGIC Source:
# MAGIC
# MAGIC https://data.cityofnewyork.us/

# COMMAND ----------

# Install brickops lib for naming and datamesh functions
%pip install brickops=0.3.16

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import os
import shutil
# Import pyspark utility functions
from pyspark.sql import functions as F

# Name functions enables automatic env+user specific database naming
from brickops.datamesh.naming import dbname
from brickops.datamesh.naming import build_table_name as tablename
from brickops.datamesh.naming import catname_from_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Copy static data from git folder to volume
# MAGIC
# MAGIC Some spark clusters can not read from local files
# MAGIC
# MAGIC * Create volume
# MAGIC * Copy csv file to volume
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create database

# COMMAND ----------

cat = catname_from_path()
db = dbname(db="revenue", cat=cat)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create file volume to store CSVs

# COMMAND ----------

volume = "static_data"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {db}.{volume}")

# COMMAND ----------

dbdir = db.replace(".", "/")
volume_path = f"/Volumes/{dbdir}/{volume}"
print(f"volume_path: {volume_path}")
print("folder contents (should be empty on first run, but not give error):")
os.listdir(volume_path)

# COMMAND ----------


filename = "New_York_City_Population_by_Borough__1950_-_2040.csv"
local_csv_path = f"static_data/{filename}"
volume_csv_path = f"{volume_path}/{filename}"
shutil.copyfile(local_csv_path, volume_csv_path)

# COMMAND ----------

pop_df = spark.read.option("header", True).csv(volume_csv_path)

# COMMAND ----------

pop_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Population per borough
# MAGIC
# MAGIC Let's use 2020 population numbers

# COMMAND ----------

simple_pop_df = (
    pop_df.select(
        F.trim(F.col("Borough")).alias("borough"),
        F.regexp_replace("2020", ",", "").alias("population").cast("int"),
        F.col("2020 - Boro share of NYC total").alias("population_share").cast("float"),
    )
)
simple_pop_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write dataset

# COMMAND ----------

catalog = catname_from_path()
print(f"catalog: {catalog}")

# COMMAND ----------

borough_population_tbl = tablename(cat=catalog, db="revenue", tbl="borough_population")
print("borough_population_tbl:" + repr(borough_population_tbl))
(
    simple_pop_df.write.mode("overwrite")
    .format("delta")
    .saveAsTable(borough_population_tbl)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Generate training dataset
# MAGIC
# MAGIC Only needed to be run once by teacher, so ignore

# COMMAND ----------

# # Used to generate training data set, needed by DLT Pipeline
# borough_population_tbl = "training.taxinyc_trips.borough_population"
# (
#     simple_pop_df.write.mode("overwrite")
#     .format("delta")
#     .saveAsTable(borough_population_tbl)
# )
