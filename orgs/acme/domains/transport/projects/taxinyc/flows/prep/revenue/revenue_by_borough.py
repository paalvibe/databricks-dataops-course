# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the revenue data product
# MAGIC #

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

# dbutils.widgets.text("jobparamfoo", "lee")
# jobparamfoo = dbutils.widgets.get("jobparamfoo")
# print(f"jobparamfoo: {jobparamfoo}")

# COMMAND ----------

def get_parameters():
    all_args = dict(dbutils.notebook.entry_point.getCurrentBindings())
    # remove '--' substring
    all_args = {key.replace('--', ''): value for key, value in all_args.items()}
    # parse values to correct format
    all_args = {key: ast.literal_eval(value) for key, value in all_args.items()}
    return all_args

params = get_parameters()
print("params: " + repr(params))

# COMMAND ----------

# context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nbpath = ctx.notebookPath().get()
print("nbpath: " + repr(nbpath))
tags = ctx.tags().get()
# context = json.loads(context_str)
# run_id_obj = context.get('currentRunId', {})
#run_id = run_id_obj.get('id', None) if run_id_obj else None
# print("run_id: " + repr(run_id))
# tags = context.get('tags', {}).get('jobId', None)
print("tags: " + repr(tags))


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

revenue_by_borough_df = (
    trips_df.groupBy("pickup_borough")
    .agg(F.sum("total_amount").alias("amount"))
    .sort(F.col("pickup_borough").asc())
)
revenue_by_borough_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write dataset

# COMMAND ----------

catalog = catname_from_path()
print(f"catalog: {catalog}")

# COMMAND ----------

revenue_by_borough_df

# COMMAND ----------

revenue_by_borough_tbl = tblname(cat=catalog, db="revenue", tbl="revenue_by_borough")
print("revenue_by_borough_tbl:" + repr(revenue_by_borough_tbl))
(
    revenue_by_borough_df.write.mode("overwrite")
    .format("delta")
    .saveAsTable(revenue_by_borough_tbl)
)

# COMMAND ----------


