# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Run pipeline
# MAGIC
# MAGIC The notebooks for producing the output data has already been developed.
# MAGIC Your next task is to run the notebooks yourself.
# MAGIC
# MAGIC This is the manual processing of the pipeline which we later will run automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## In and out data
# MAGIC
# MAGIC ### Incoming data
# MAGIC
# MAGIC - A sample of NYC Taxi data, from Unity Catalog (UC) path `training.taxinyc_trips.yellow_taxi_trips_curated_sample`
# MAGIC - NYC population data
# MAGIC
# MAGIC ### Outgoing data product
# MAGIC
# MAGIC We produce a data product called revenue. For now it contains the following outgoing data sets:
# MAGIC
# MAGIC 1. `revenue_by_tripmonth`
# MAGIC 2. `revenue_by_borough`
# MAGIC 3. `revenue_per_inhabitant`
# MAGIC
# MAGIC `borough_population` is an intermediate internal dataset.
# MAGIC
# MAGIC In production, the data product will be written as the UC schema `acme_transport_taxinyc.revenue`.
# MAGIC
# MAGIC ## Task: Run the notebooks manually.
# MAGIC
# MAGIC For each notebook, connect to Serverless, and run through the cells. Study the cells as you run them.
# MAGIC
# MAGIC Go to `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/`
# MAGIC
# MAGIC 1. Run notebook: `borough_population`
# MAGIC 2. Run notebook: `revenue_by_tripmonth`
# MAGIC 3. Run notebook: `revenue_by_borough`
# MAGIC 4. Run notebook: `revenue_per_inhabitant`. It depends on 1 and 3 completing first.
