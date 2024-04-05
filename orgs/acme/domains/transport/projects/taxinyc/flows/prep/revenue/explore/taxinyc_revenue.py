# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore the revenue data product
# MAGIC #

# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F

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

revenue_by_borough_df = (
    trips_df.groupBy("pickup_borough")
    .agg(F.sum("total_amount").alias("amount"))
    .sort(F.col("pickup_borough").asc())
)
revenue_by_borough_df.display()

# COMMAND ----------


