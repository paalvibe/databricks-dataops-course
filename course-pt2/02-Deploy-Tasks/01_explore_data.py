# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explore data
# MAGIC
# MAGIC In this exercise we will explore the data we will work with.
# MAGIC
# MAGIC ## Data development lifecycle
# MAGIC
# MAGIC A data pipeline is a sequence of transformations to transform data sources into new data sets.
# MAGIC
# MAGIC Building data pipelines often consist of these phases:
# MAGIC
# MAGIC 1. Explore data
# MAGIC 2. Build data transformations and pipeline
# MAGIC 3. Test data pipeline
# MAGIC 4. Propose data pipeline for production deployment
# MAGIC     - With a pull request
# MAGIC     - Triggers a staging test run
# MAGIC 5. Deploy the pipeline to production
# MAGIC
# MAGIC Step 4 and 5 should be done with CICD and something like Github actions. We will not implement that in this course, but instead invoke the python functions needed to by those actions, especially for deploying new Databricks Workflows (also called Jobs).
# MAGIC
# MAGIC ## Data exploration
# MAGIC
# MAGIC It is sometimes called Exploratory Data Analysis (EDA), but the `explore` folder can also be used for snippets exploring how to explore the data.
# MAGIC
# MAGIC The `explore` folder should never be used in pipelines. However, it can still be useful to allow for exploratory code to be git versioned, which is why it is part of folder structure.
# MAGIC
# MAGIC Right now we will see how to explore data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Explore data
# MAGIC
# MAGIC Open a new tab, and go to `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/explore/taxinyc_revenue`. Connect to the Serverless cluster, and run through the cells.
