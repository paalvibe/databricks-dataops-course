# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy prod job
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Redeploy without changes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Go to the notebook orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy, and go to the run prod job cell.
# MAGIC
# MAGIC Run the cell with autojob() with env="prod"
# MAGIC
# MAGIC The run the job with the python function.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Study the name of the prod job and prod output data
# MAGIC
# MAGIC Ideally, the data should be written to a schema without prefix, since it is production. But we are working on the implementation. Trying to solve how to pickup up env vars signalling pipeline env prod vs dev.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...
