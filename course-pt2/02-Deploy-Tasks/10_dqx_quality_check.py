# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Quality checks
# MAGIC
# MAGIC [DQX](https://databrickslabs.github.io/dqx/) is a very promising new data quality frameworks
# MAGIC
# MAGIC * Can run in both DLT and regular spark notebooks (unlike DLT quality checks)
# MAGIC * Has [profiling](https://databrickslabs.github.io/dqx/docs/guide/#data-profiling-and-quality-rules-generation) and a lot of other nice functionality, like quality dashboards
# MAGIC * Official Databricks Labs framework
# MAGIC * Checks can be written in python, yaml or json
# MAGIC
# MAGIC When to use DQX (according to DQX team)
# MAGIC
# MAGIC * Use DQX if you need pro-active monitoring (before data is written to a target table).
# MAGIC * For monitoring data quality of already persisted data in Delta tables (post-factum monitoring), try Databricks Lakehouse Monitoring.
# MAGIC * DQX can be integrated with DLT for data quality checking but your first choice for DLT pipelines should be DLT Expectations. DQX can be used to profile data and generate DLT expectation candidates.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run revenue_per_inhabitant with DQX quality checks
# MAGIC
# MAGIC Go to the notebook `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue_per_inhabitant_w_dqx`.
# MAGIC
# MAGIC Connect to Serverless compute and run all cells.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Why are there no rows in the valid_df data set?
# MAGIC
# MAGIC Hint: percentage is stored as a number between 0-100.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Fix the failing quality checks and get the pipeline to run
# MAGIC
# MAGIC It should produce five rows of output.

# COMMAND ----------


