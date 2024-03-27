# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Create a student-specific database<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the taxi_db database in Databricks
# MAGIC
# MAGIC Specific for the student

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Create database

# COMMAND ----------

from libs.dbname import dbname
taxi_db = dbname(db="taxi_db")
print("New db name: " + taxi_db)
spark.conf.set("nbvars.taxi_db", taxi_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.taxi_db};

# COMMAND ----------

from libs.dbname import dbname
crime_db = dbname(db="crime")
print("New db name: " + crime_db)
spark.conf.set("nbvars.crime_db", crime_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.crime_db};

# COMMAND ----------

from libs.dbname import dbname
books_db = dbname(db="books")
print("New db name: " + books_db)
spark.conf.set("nbvars.books_db", books_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.books_db};

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG training;
# MAGIC SHOW DATABASES;
