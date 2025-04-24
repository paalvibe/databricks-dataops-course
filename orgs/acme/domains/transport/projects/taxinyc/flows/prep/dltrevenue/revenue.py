# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DLT pipeline as a single notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imports

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Trips

# COMMAND ----------

@dlt.table
def trips():
    return spark.table("training.taxinyc_trips.yellow_taxi_trips_curated_sample")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Revenue by borough

# COMMAND ----------

@dlt.table(
    name="revenue_by_borough",
    comment="Aggregated revenue by pickup borough",
    table_properties={"quality": "silver"}
)
def revenue_by_borough():
    return (
        dlt.read("trips").groupBy("pickup_borough")
        .agg(F.sum("total_amount").alias("amount"))
        .sort(F.col("pickup_borough").asc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Revenue by trip month

# COMMAND ----------

@dlt.table(
    name="revenue_by_tripmonth",
    comment="Aggregated revenue by trip month",
    table_properties={"quality": "silver"}
)
def revenue_by_tripmonth():
    return (
        dlt.read("trips").groupBy("pickup_borough", "trip_year", "trip_month")
        .agg(F.sum("total_amount").alias("amount"))
        .sort(
        ["pickup_borough", "trip_year", "trip_month"], ascending=True
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Revenue per inhabitant

# COMMAND ----------

@dlt.table(
    name="borough_population",
    comment="Population per borough",
    table_properties={"quality": "silver"}
)
def borough_population():
    return spark.table("training.taxinyc_trips.borough_population")


@dlt.table(
    name="revenue_per_inhabitant",
    comment="Aggregated revenue by pickup borough per inhabitant",
    table_properties={"quality": "silver"}
)
def revenue_per_inhabitant():
    revenue_by_borough_df = dlt.read("revenue_by_borough")
    borough_population_df = dlt.read("borough_population")
    return (
        revenue_by_borough_df
        .where(F.col("pickup_borough").isNotNull() & (F.col("pickup_borough") != "Unknown"))
        .join(borough_population_df, revenue_by_borough_df.pickup_borough == borough_population_df.borough, "inner")
        .withColumn("amount", F.round("amount", 2))
        .withColumn("revenue_per_inhabitant", F.round(
            F.col("amount") / borough_population_df.population, 2)
        )
        .sort("revenue_per_inhabitant", ascending=False)
    )
