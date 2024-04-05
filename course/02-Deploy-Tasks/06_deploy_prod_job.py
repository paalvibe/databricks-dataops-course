# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Redeploy
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task change the name of one of the output data sets
# MAGIC
# MAGIC One way is to use the `alias()` function in pyspark for renaming.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Redeploy without changes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Go to the notebook orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy, and run the autojob cell again.
# MAGIC
# MAGIC Note that the output will say `Resetting job` instead of `Creating job`.
# MAGIC
# MAGIC Look in the Workflows page. The job should be there still, but it has been updated.
# MAGIC
# MAGIC Notice that the code that is being deployed is the code in git, not the state of the code in your repo, so your output data does not reflect the changes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Redeploy with new commit
# MAGIC
# MAGIC Do a commit and run the autodeploy() fn again.
# MAGIC Check the result under Workflows. Why is there a new job created?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...
