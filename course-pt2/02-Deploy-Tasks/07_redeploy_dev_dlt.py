# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Redeploy
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task change the name of amount to full_amount in revenue_per_inhabitant data set
# MAGIC
# MAGIC One way is to use the `alias()` function in pyspark for renaming.
# MAGIC
# MAGIC E.g. from
# MAGIC
# MAGIC ```
# MAGIC .withColumn("amount", F.round("amount", 2))
# MAGIC ```
# MAGIC
# MAGIC to:
# MAGIC
# MAGIC ```
# MAGIC .withColumn("full_amount", F.round("amount", 2))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Redeploy without changes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Go to the notebook orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/deploy, and run the autopipeline() cell again.
# MAGIC
# MAGIC Note that it will return only the name, but not the pipeline id, since the pipeline has been updated instead of created.
# MAGIC
# MAGIC Look in the Workflows page. The job should be there still, but it has been updated.
# MAGIC
# MAGIC Notice that the code that is being deployed is the code in git, not the state of the code in your repo, so your output data does not reflect the changes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Redeploy with new commit
# MAGIC
# MAGIC Do a commit and run the autopipeline() fn again.
# MAGIC Check the result under Pipelines. Why is there a new pipeline created?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Answer here...
