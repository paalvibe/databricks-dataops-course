# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create a dlt policy and give permissions to the training group, under Compute -> Policies.
# MAGIC
# MAGIC Policy spec:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_type": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "dlt"
# MAGIC   },
# MAGIC   "num_workers": {
# MAGIC     "type": "unlimited",
# MAGIC     "defaultValue": 3,
# MAGIC     "isOptional": true
# MAGIC   },
# MAGIC   "node_type_id": {
# MAGIC     "type": "unlimited",
# MAGIC     "isOptional": true
# MAGIC   },
# MAGIC   "spark_version": {
# MAGIC     "type": "unlimited",
# MAGIC     "hidden": true
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------


