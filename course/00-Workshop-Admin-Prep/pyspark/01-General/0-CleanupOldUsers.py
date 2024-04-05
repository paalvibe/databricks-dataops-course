# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog training;

# COMMAND ----------

# from pyspark.sql import SparkSession

# create a SparkSession
# spark = SparkSession.builder.appName("ShowTablesInfo").getOrCreate()

# # set the database
# spark.catalog.setCurrentDatabase("training")

# # get all tables
# tables = spark.catalog.listTables()

# # loop through tables and display database, table, and location
# for table in tables:
#     database = table.database
#     name = table.name
#     location = spark.sql(f"DESCRIBE EXTENDED {name}").filter("Location").select("data_type").collect()[0][0]
#     print(f"Database: {database}, Table: {name}, Location: {location}")

# # stop the SparkSession
# spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop all schemas prefixed with `dev_`

# COMMAND ----------

schemas = spark.catalog.listDatabases()
first_schema = None
for schema in schemas:
    first_schema = schema
    # print(schema['name'])
    if schema.name.startswith("dev_"):
        print(f"Drop schema {schema.name}")
        spark.sql(f"DROP SCHEMA {schema.name} CASCADE")
first_schema

# COMMAND ----------


