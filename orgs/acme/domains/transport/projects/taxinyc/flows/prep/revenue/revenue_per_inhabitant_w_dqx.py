# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the revenue data product and do data quality check with DQX
# MAGIC #

# COMMAND ----------

# MAGIC %pip install brickops=0.3.16

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dqx library

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx==0.1.12

# COMMAND ----------

# reload libs
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Install other dependencies

# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F

# Name functions enables automatic env+user specific database naming
from brickops.datamesh.naming import build_table_name as tablename
from brickops.datamesh.naming import catname_from_path

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

revenue_by_borough_tbl = tablename(cat=catalog, db="revenue", tbl="revenue_by_borough")
print("revenue_by_borough_tbl:" + repr(revenue_by_borough_tbl))
revenue_by_borough_df = spark.sql(f"select * from {revenue_by_borough_tbl}")
revenue_by_borough_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get borough population

# COMMAND ----------

borough_population_tbl = tablename(cat=catalog, db="revenue", tbl="borough_population")
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
    .withColumn("amount", F.round("amount", 2))
    .withColumn("revenue_per_inhabitant", F.round(
        revenue_by_borough_df.amount / borough_population_df.population, 2)
    )
    .sort("revenue_per_inhabitant", ascending=False)
)
revenue_per_inhabitant_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Do quality checks with DQX

# COMMAND ----------

from databricks.labs.dqx.col_functions import is_in_range, not_less_than
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRuleColSet, DQRule
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())
checks = [
    DQRule(
        name="population_share",
        criticality="error",
        check=is_in_range("population_share", 0, 1)),
    DQRule(
        name="non_negative_amount",
        criticality="error",
        check=not_less_than("amount", 0)),
]

# apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantined_df = dq_engine.apply_checks_and_split(revenue_per_inhabitant_df, checks)

# COMMAND ----------

display(quarantined_df)

# COMMAND ----------

display(valid_df)

# COMMAND ----------

valid_df.count()

# COMMAND ----------

# For demonstrative purposes we fail hard if we don't get five records in the valid dataset
assert valid_df.count() == 5

# COMMAND ----------

revenue_per_inhabitant_tbl = valid_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write dataset

# COMMAND ----------

revenue_per_inhabitant_tbl = tablename(cat=catalog, db="revenue", tbl="revenue_per_inhabitant")
print("revenue_per_inhabitant_tbl:" + repr(revenue_per_inhabitant_tbl))
(
    revenue_per_inhabitant_df.write.mode("overwrite")
    .option("mergeSchema", True)
    .format("delta")
    .saveAsTable(revenue_per_inhabitant_tbl)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks with DQX
# MAGIC
# MAGIC DQX is a data quality framework for Apache Spark that enables you to define, monitor, and react to data quality issues in your data pipelines.
# MAGIC
# MAGIC [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)
