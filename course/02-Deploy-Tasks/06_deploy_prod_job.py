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
# MAGIC
# MAGIC **hint** : this will not work, can you think of any reasons why this is the case?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Study the name of the prod job and prod output data
# MAGIC
# MAGIC The brickops package will detect whether the deployment is headed to prod or not, and will remove any prefixes / suffixes, but the code needs to be deployed with a service principal for this to work.
# MAGIC Any proposals for customizations are most welcome :)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...
