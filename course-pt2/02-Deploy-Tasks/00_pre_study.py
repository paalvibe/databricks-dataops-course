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
# MAGIC `orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/`
# MAGIC
# MAGIC The structure is: 
# MAGIC - org: `acme`
# MAGIC     - domain: `transport`
# MAGIC         - project: `taxinyc`
# MAGIC             - flowtype: `prep` (meaning ETL/data engineering, the alternative is `ml`, for ML work)
# MAGIC                 - flow: `dltrevenue` 
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
# MAGIC ## Study our naming configuration
# MAGIC
# MAGIC The resource (catalog, table, job etc) naming is created by the [brickops](https://github.com/brickops/brickops) library.
# MAGIC Look at our brickops config in `/.brickopscfg/config.yml` now to see how we have defined our naming.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Topic: Notebook shortcuts
# MAGIC
# MAGIC Take a quick look at the shortcuts for notebooks by pressing Help->Shortcuts in the menu, or pressing `h`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Pro-tip: Navigate between files with the Recents menu option
# MAGIC
# MAGIC On the left side menu you can use the `Recents` link to easily jump between recent notebooks.
# MAGIC You might also see tabs on the top of this page, which enables you to navigate.
