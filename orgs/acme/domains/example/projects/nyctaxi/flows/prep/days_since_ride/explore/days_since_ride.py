# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Load taxi data

# COMMAND ----------

import datetime
from datetime import date
from pyspark.sql import functions as F

# COMMAND ----------

import sys
# TODO Find better solution for this
sys.path.append("/Workspace/Repos/user@foofoo.foo/")
# sys.path.append('../../libs/dataframe/')

# COMMAND ----------

# Enable importing libs in an orderly manner
from repotools.projpath import add_project_dir_to_syspath
add_project_dir_to_syspath()

# COMMAND ----------

from libs.age import gen_age

# COMMAND ----------

taxi_data_df = table("default.taxi_2019_12")

# COMMAND ----------

taxi_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prepare pickup date column

# COMMAND ----------

w_pu_dt_df = taxi_data_df.withColumn('pickup_date', F.to_date('Pickup_DateTime'))
w_pu_dt_df.select('Pickup_DateTime', 'pickup_date', 'Passenger_Count').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Use gen_age lib to generate trip age

# COMMAND ----------

w_trip_age = gen_age(df=w_pu_dt_df, today=date(2022, 12, 10), datecol="pickup_date")

# COMMAND ----------

w_trip_age.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Display dataset selection with age

# COMMAND ----------

w_trip_age.select('Pickup_DateTime', 'pickup_date', 'age', 'Passenger_Count').display()

# COMMAND ----------

from libs.visualize import plot_distributions

# COMMAND ----------

wo_null_counts = w_trip_age.limit(400).where(F.col("Passenger_count").isNotNull())
fig = plot_distributions(w_trip_age, ['Passenger_Count'])

# COMMAND ----------


