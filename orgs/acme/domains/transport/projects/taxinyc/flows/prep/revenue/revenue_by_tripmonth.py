# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the revenue data product
# MAGIC #

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------



# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F

# Name functions enables automatic env+user specific database naming
from libs.tblname import tblname
from libs.catname import catname_from_path
from libs.dbname import dbname

# COMMAND ----------

cat = catname_from_path()
db = dbname(db="revenue", cat=cat)
print("New db name: " + db)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get input dataset
# MAGIC ...and show a few records of input dataset

# COMMAND ----------

trips_df = spark.sql(
    "select * from training.taxinyc_trips.yellow_taxi_trips_curated_sample"
)
trips_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Revenue sum by pickup_borough
# MAGIC
# MAGIC `total_amount` is the revenue for a trip.
# MAGIC
# MAGIC Sort by pickup_borough ascending.

# COMMAND ----------

revenue_by_trip_month_df = (
    trips_df.groupBy("pickup_borough", "trip_year", "trip_month")
    .agg(F.sum("total_amount").alias("amount"))
    .sort(
        ["pickup_borough", "trip_year", "trip_month"], ascending=True
        )
)
revenue_by_trip_month_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write dataset

# COMMAND ----------

catalog = catname_from_path()
print(f"catalog: {catalog}")

# COMMAND ----------

revenue_by_trip_month_df

# COMMAND ----------

revenue_by_trip_month_tbl = tblname(cat=catalog, db="revenue", tbl="revenue_by_tripmonth")
print("revenue_by_trip_month_tbl:" + repr(revenue_by_trip_month_tbl))
(
    revenue_by_trip_month_df.write.mode("overwrite")
    .format("delta")
    .saveAsTable(revenue_by_trip_month_tbl)
)

# COMMAND ----------


