# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Pre-study
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Topic: Repository structure
# MAGIC
# MAGIC We will do our tasks in the context of the folder representing the revenue data pipeline or flow:
# MAGIC
# MAGIC `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/`
# MAGIC
# MAGIC The structure is: 
# MAGIC - org: `acme`
# MAGIC     - domain: `transport`
# MAGIC         - project: `taxinyc`
# MAGIC             - flowtype: `prep` (meaning ETL/data engineering, the alternative is `ml`, for ML work)
# MAGIC                 - flow: `revenue` 
# MAGIC
# MAGIC The structure will be applied to:
# MAGIC
# MAGIC - Data *code*, i.e. the pyspark code herein git
# MAGIC - The database *tables* produced by that code
# MAGIC - The data pipelines being deployed
# MAGIC
# MAGIC The purpose of this structure is to have sufficient granularity to enable each department/org, team/domain, project and pipeline, to be kept apart.
# MAGIC
# MAGIC You can explore the structure here in Databricks, or more easily [in the repo with a browser](https://github.com/paalvibe/databricks-dataops-course).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Topic: Notebook shortcuts
# MAGIC
# MAGIC Take a quick look at the shortcuts for notebooks by pressing Help->Shortcuts in the menu, or pressing `h`.

# COMMAND ----------


