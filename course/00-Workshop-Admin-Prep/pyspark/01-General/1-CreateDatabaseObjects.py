# Databricks notebook source
# MAGIC %md
# MAGIC # Setup database objects for the workshop
# MAGIC
# MAGIC 1) Run through the cells as workspace admin
# MAGIC 2) Grant use on POLICY `dlt_custom_policy` to `banenor-workshop` or whatever group is doing the course, under Compute->Policies. I have not found a way of doing it with sql, API could probably be use.
# MAGIC
# MAGIC You will also need a policy like this to be created first:
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

# Set groupname as name of the group of users doing the workshop
groupname = "dataops-workshop"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# display(spark.catalog.listDatabases())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create training catalog
# MAGIC * The cluster needs shared access mode, to be able to use unity catalog
# MAGIC   * Notice that you PT need to create the cluster in unrestricted mode, to enable shared access mode. Otherwise, you cannot work with unity catalog.
# MAGIC * The admin user running this needs needs access to create catalogs in the metastore. One way is to make the user metastore admin in https://accounts.cloud.databricks.com/

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS training")

# COMMAND ----------

spark.sql(f"GRANT CREATE SCHEMA ON CATALOG training TO `{groupname}`")

# COMMAND ----------

spark.sql(f"GRANT USE_SCHEMA ON SCHEMA training.data TO `{groupname}`")

# COMMAND ----------

spark.sql(f"GRANT READ VOLUME ON VOLUME training.data.crimes TO `{groupname}`")

# COMMAND ----------

spark.sql(f"GRANT WRITE VOLUME ON VOLUME training.data.crimes TO `{groupname}`")

# COMMAND ----------

# MAGIC %md
# MAGIC # Grant select on files to users
# MAGIC
# MAGIC Needed to avoid `java.lang.SecurityException: User does not have permission SELECT on any file.`
# MAGIC when reading data from /mnt/ areas.

# COMMAND ----------

spark.sql(f"GRANT SELECT ON ANY FILE TO `users`")
spark.sql(f"GRANT MODIFY ON ANY FILE TO `users`")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup domain catalog

# COMMAND ----------

# Create full mesh catalog
domain_catalog = "acme_transport_taxinyc"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {domain_catalog}")
spark.sql(f"GRANT USE CATALOG ON CATALOG {domain_catalog} TO `{groupname}`")
spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {domain_catalog} TO `{groupname}`")

# COMMAND ----------

# Create reduced mesh (without org and proejct) catalog
domain_catalog = "transport"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {domain_catalog}")
spark.sql(f"GRANT USE CATALOG ON CATALOG {domain_catalog} TO `{groupname}`")
spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {domain_catalog} TO `{groupname}`")

# COMMAND ----------


