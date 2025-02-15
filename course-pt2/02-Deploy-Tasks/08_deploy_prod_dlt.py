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
# MAGIC Go to the notebook orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/deploy, and go to the run prod pipeline cell.
# MAGIC
# MAGIC Run the cell with autopipeline() with env="prod"
# MAGIC
# MAGIC The run the pipeline with the python function.
# MAGIC
# MAGIC **hint** : this will not work, can you think of any reasons why this is the case?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Study the name of the prod pipeline and prod output data
# MAGIC
# MAGIC Ideally, the data should be written to a schema without prefix, since it is production. But we are working on the implementation. Trying to solve how to pickup up env vars signalling pipeline env prod vs dev.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...
