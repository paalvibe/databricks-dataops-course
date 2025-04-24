# Databricks notebook source
# MAGIC %md
# MAGIC # Data Contract example with `datacontract-cli`
# MAGIC
# MAGIC The datacontract CLI is an open-source command-line tool for working with data contracts. It uses data contract YAML files as [Data Contract Specification](https://datacontract.com/) or [ODCS](https://bitol-io.github.io/open-data-contract-standard/latest/) to lint the data contract, connect to data sources and execute schema and quality tests, detect breaking changes, and export to different formats. The tool is written in Python. It can be used as a standalone CLI tool, in a CI/CD pipeline, or directly as a Python library.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task: Study the data contract
# MAGIC
# MAGIC How does it relate to the table definition of `revenue_per_inhabitant`?

# COMMAND ----------

# MAGIC %md
# MAGIC Answer here...

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Verify contract

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Go to the notebook `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue_w_contract/verify_contract`, and run through it gradually, with `Serverless` cluster.
# MAGIC
# MAGIC If you get python version problems, you can try connecting to the `contract_checker` cluster instead, using the `verify_contract_on_cluster` notebook instead of `verify_contract`.
# MAGIC Note that an SQL warehouse must be available at `/sql/1.0/warehouses/aee0a674651b7e21`. See SQL Warehouses on the left side menu.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bonus Task: Verify the contract on your local environment
# MAGIC
# MAGIC 1. Check out the repo.
# MAGIC 2. Install `Pipenv`.
# MAGIC 3. Run `pipenv install`.
# MAGIC 4. Run `pipenv shell`.
# MAGIC 5. got to `orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue_w_contract`
# MAGIC 6. Run `datacontract test ./contracts/revenue_per_inhabitant.yaml`
# MAGIC 7. You can also try `python tools/verify_contract.py`
# MAGIC
# MAGIC You will need these three env vars defined:
# MAGIC
# MAGIC ```
# MAGIC DATACONTRACT_DATABRICKS_SERVER_HOSTNAME like `/sql/1.0/warehouses/aee0a674651b7e21`
# MAGIC DATACONTRACT_DATABRICKS_HTTP_PATH
# MAGIC DATACONTRACT_DATABRICKS_TOKEN
# MAGIC ```
