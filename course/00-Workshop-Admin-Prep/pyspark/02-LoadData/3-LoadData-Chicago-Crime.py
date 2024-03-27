# Databricks notebook source
# MAGIC %md
# MAGIC # DBFS - read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  **Download** and curate Chicago crimes public dataset  - 1.5 GB of the Chicago crimes public dataset - has 6.7 million records.<BR>
# MAGIC 2.  **Upload the dataset to DBFS**, to the staging directory in DBFS<BR>
# MAGIC 3.  Read the CSV into a dataframe, **persist as parquet** to the raw directory<BR>
# MAGIC 4.  **Create an external table** on top of the dataset in the raw directory<BR>
# MAGIC 5.  **Explore with SQL construct**<BR>
# MAGIC 6.  **Curate** the dataset (dedupe, add additional dervived attributes of value etc) for subsequent labs<BR>
# MAGIC 7.  Do some basic **visualization**<BR>
# MAGIC   
# MAGIC Chicago crimes dataset:<br>
# MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
# MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
# MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>
# MAGIC   
# MAGIC Referenes for Databricks:<br>
# MAGIC Working with blob storage: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html <br>
# MAGIC Visualization: https://docs.databricks.com/user-guide/visualizations/charts-and-graphs-scala.html
# MAGIC   

# COMMAND ----------

# MAGIC %sh
# MAGIC # 1) Download dataset - gets downloaded to driver
# MAGIC wget -O /tmp/chicago-crimes.csv "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"

# COMMAND ----------

# MAGIC %ls -l /tmp/chicago-crimes.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Upload from driver node to DBFS

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /Volumes/training/data/crimes/staging/chicago-crimes

# COMMAND ----------

# 1) Create destination directory

# Only do this once!!!

# dbfs_dir_path = f"/Volumes/training/data/crimes/staging/chicago-crimes"
# dbutils.fs.rm(dbfs_dir_path, recurse=True)
# dbutils.fs.mkdirs(dbfs_dir_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC cp -r /tmp/chicago-crimes.csv /Volumes/training/data/crimes/staging/chicago-crimes

# COMMAND ----------

# MAGIC %ls -l /Volumes/training/data/crimes/staging/chicago-crimes

# COMMAND ----------

# # 2) Upload to from localDirPath to dbfs_dir_path
# dbutils.fs.cp("file:/tmp/chicago-crimes.csv", dbfs_dir_path, recurse=True)

# # 3) Clean up local directory
# # dbutils.fs.rm(localFile)

# # 4) List dbfs_dir_path
# display(dbutils.fs.ls(dbfs_dir_path))

# COMMAND ----------


