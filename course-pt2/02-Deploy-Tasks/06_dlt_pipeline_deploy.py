# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Pre-study
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Topic: Repository structure
# MAGIC
# MAGIC We will do our tasks in the context of the folder representing the revenue data pipeline or flow:
# MAGIC
# MAGIC `orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/`
# MAGIC
# MAGIC The structure is: 
# MAGIC - org: `acme`
# MAGIC     - domain: `transport`
# MAGIC         - project: `taxinyc`
# MAGIC             - flowtype: `prep` (meaning ETL/data engineering, the alternative is `ml`, for ML work)
# MAGIC                 - flow: `dltrevenue` 
# MAGIC
# MAGIC The structure will be applied to:
# MAGIC
# MAGIC - Data *code*, i.e. the pyspark code herein git
# MAGIC - The database *tables* produced by that code
# MAGIC - The data pipelines being deployed
# MAGIC
# MAGIC The purpose of this structure is to have sufficient granularity to enable each department/org, team/domain, project and pipeline, to be kept apart.
# MAGIC
# MAGIC You can explore the structure here in Databricks, or more easily [in the repo with a browser](https://github.com/paalvibe/databricks-dataops-course).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Topic: Notebook shortcuts
# MAGIC
# MAGIC Take a quick look at the shortcuts for notebooks by pressing Help->Shortcuts in the menu, or pressing `h`.

# COMMAND ----------

json_conf = {
    "id": "6179fc4a-0eb8-4427-9f23-3e59ca2c9197",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "policy_id": "000B840724D34AFB",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        },
        {
            "label": "maintenance",
            "policy_id": "000B840724D34AFB"
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/paal.de.vibe@entur.org/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue_w_dq/revenue_w_dq"
            }
        }
    ],
    "name": "dltrevenue_w_dq_paaldevibe_manual_test",
    "edition": "ADVANCED",
    "catalog": "acme_transport_taxinyc",
    "target": "dev_paaldevibe_gh123venice_98d4b0eb_dltrevenue_w_dq",
    "data_sampling": false
}
