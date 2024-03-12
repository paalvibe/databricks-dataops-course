# Databricks notebook source
import sys
import os

# Code to compare to variations of a data set

# In the command below, replace <username> with your Databricks user name.
sys.path.append('../../libs/dataframe/')

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import dfdiff

# COMMAND ----------

df2 = spark.sql("SELECT * FROM nyccopy.trips_aggregation_2")

# COMMAND ----------

df3 = spark.sql("SELECT * FROM nyccopy.trips_aggregation_3")

# COMMAND ----------

dfdiff.exponential_diff

# COMMAND ----------

diff_ret = dfdiff.exponential_diff(expected=df2, actual=df3)
diff_ret

# COMMAND ----------

dfdiff.print_row_sets(expected=diff_ret['rows']['rows_not_in_actual'], actual=diff_ret['rows']['rows_not_in_expected'])

# COMMAND ----------

dfdiff.print_diff_first_rows(diff_ret['rows']['rows_not_in_expected'], diff_ret['rows']['rows_not_in_actual'])

# COMMAND ----------


