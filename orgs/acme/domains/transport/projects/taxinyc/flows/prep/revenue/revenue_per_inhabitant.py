# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the revenue data product
# MAGIC #

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F

# Name functions enables automatic env+user specific database naming
from libs.tblname import tblname
from libs.catname import catname_from_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get input datasets
# MAGIC ...and show a few records of input dataset

# COMMAND ----------

catalog = catname_from_path()
print(f"catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Get revenue by borough

# COMMAND ----------

revenue_by_borough_tbl = tblname(cat=catalog, db="revenue", tbl="revenue_by_borough")
print("revenue_by_borough_tbl:" + repr(revenue_by_borough_tbl))
revenue_by_borough_df = spark.sql(f"select * from {revenue_by_borough_tbl}")
revenue_by_borough_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get borough population

# COMMAND ----------

borough_population_tbl = tblname(cat=catalog, db="revenue", tbl="borough_population")
print("borough_population_tbl:" + repr(borough_population_tbl))
borough_population_df = spark.sql(f"select * from {borough_population_tbl}")
borough_population_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Revenue per inhabitant
# MAGIC
# MAGIC `total_amount_per_inhabitant` total revenue divided by population.
# MAGIC
# MAGIC Discard records where pickup_borough is null or unknown.

# COMMAND ----------

revenue_per_inhabitant_df = (
    revenue_by_borough_df
    .where(F.col("pickup_borough").isNotNull() & (F.col("pickup_borough") != "Unknown"))
    .join(borough_population_df, revenue_by_borough_df.pickup_borough == borough_population_df.borough, "inner")
    .withColumn("rounded_amount", F.round("amount", 2))
    .withColumn("revenue_per_inhabitant", F.round(
        revenue_by_borough_df.amount / borough_population_df.population, 2)
    )
    .sort("revenue_per_inhabitant", ascending=False)
)
revenue_per_inhabitant_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write dataset

# COMMAND ----------

revenue_per_inhabitant_tbl = tblname(cat=catalog, db="revenue", tbl="revenue_per_inhabitant")
print("revenue_per_inhabitant_tbl:" + repr(revenue_per_inhabitant_tbl))
(
    revenue_per_inhabitant_df.write.mode("overwrite")
    .option("mergeSchema", True)
    .format("delta")
    .saveAsTable(revenue_per_inhabitant_tbl)
)

# COMMAND ----------


