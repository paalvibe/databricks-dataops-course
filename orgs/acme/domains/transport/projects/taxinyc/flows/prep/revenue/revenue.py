# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the revenue data product
# MAGIC #

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show a few records

# COMMAND ----------

trips_df = spark.sql(
    "select * from training.taxinyc_trips.yellow_taxi_trips_curated_sample"
)
trips_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Revenue sum by pickup_borough
# MAGIC
# MAGIC `total_amount` is the revenue for a trip.
# MAGIC
# MAGIC Sort by pickup_borough ascending.
# MAGIC
# MAGIC To sort / order by, use `.sort(F.col("col_name").asc())`.

# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F

# Name functions enables automatic env+user specific database naming
from libs.dbname import dbname
from libs.tblname import tblname, username
from libs.catname import catname_from_path

uname = username(dbutils)

# COMMAND ----------

revenue_by_borough_df = (
    trips_df.groupBy("pickup_borough")
    .agg(F.sum("total_amount"))
    .sort(F.col("pickup_borough").asc())
)
revenue_by_borough_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC % # Write dataset

# COMMAND ----------

# acme/domains/transport/projects/taxinyc

# catalog = catname(org="acme",
#                   domain="transport",
#                   project="taxinyc")
catalog = catname_from_path()
print("catalog: {catalog}")
revenue_by_borough_tbl = tblname(cat=catalog, db="revenue", tbl="revenue_by_borough")
print("revenue_by_borough_tbl:" + repr(revenue_by_borough_tbl))
(
    revenue_by_borough_df.write.mode("overwrite")
    .format("delta")
    .saveAsTable(revenue_by_borough_tbl)
)
