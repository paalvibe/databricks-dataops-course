# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Deploy pipelines defined by deployment.yml

# COMMAND ----------

!pip install git+https://github.com/brickops/brickops.git@ef74c32220b2e2d561e5150bcc380fd9766b740c

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Import libs

# COMMAND ----------

# # Enable live reloading of libs, not needed now
# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import requests
from brickops.dataops.deploy.autopipeline import autopipeline
from brickops.dataops.pipeline import run_pipeline_by_name, run_pipeline

# COMMAND ----------

# Name functions enables automatic env+user specific database naming
from brickops.datamesh.naming import catname_from_path
from brickops.datamesh.naming import dbname

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Unity catalog database for staging run
# MAGIC A Catalog will be created prefixed with username, branch and has commit

# COMMAND ----------

cat = catname_from_path()
db = dbname(db="dltrevenue", cat=cat)
print("New db name: " + db)
spark.sql(f"USE catalog {cat}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy DLT pipeline

# COMMAND ----------

from brickops.databricks.context import DbContext, current_env, get_context
db_context = get_context()
db_context

# COMMAND ----------

current_env(ctx)

# COMMAND ----------

from brickops.dataops.deploy.repo import git_source
git_source(db_context)

# COMMAND ----------

from brickops.dataops.deploy.pipeline.buildconfig import build_pipeline_config
from brickops.dataops.deploy.readconfig import read_config_yaml

cfgyaml = "deployment.yml"
env="dev"
cfg = read_config_yaml(cfgyaml)
cfg["git_source"] = git_source(db_context)
pipeline_config = build_pipeline_config(
        cfg=cfg,
        env=env,
        db_context=db_context,
)

# COMMAND ----------

from brickops.databricks.context import DbContext
from brickops.databricks.username import get_username
from brickops.datamesh.naming import pipelinename
from brickops.dataops.deploy.pipeline.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)
from brickops.gitutils import clean_branch, commit_shortref


# COMMAND ----------

from typing import Any

def depname(*, db_context: DbContext, env: str, git_src: dict[str, Any]) -> str:
    """Compose deployment name from env and git config."""
    if env == "prod":
        return "prod"
    uname = get_username(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{env}_{uname}_{branch}_{short_ref}"

def build_context_parameters(env: str, tags: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a list of parameters containing the environment and git info."""
    return [
        {
            "name": "pipeline_env",
            "default": env,
        },
        {
            "name": "git_url",
            "default": tags["git_url"],
        },
        {
            "name": "git_branch",
            "default": tags["git_branch"],
        },
        {
            "name": "git_commit",
            "default": tags["git_commit"],
        },
    ]


def _tags(*, cfg: dict[str, Any], depname: str, pipeline_env: str) -> dict[str, Any]:
    return {
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
        "pipeline_env": pipeline_env,
    }


# COMMAND ----------

full_cfg = defaultconfig()
full_cfg.update(cfg)
full_cfg.name = pipelinename(db_context, env=env)
dep_name = depname(db_context=db_context, env=env, git_src=full_cfg.git_source)
tags = _tags(cfg=cfg, depname=dep_name, pipeline_env=env)
full_cfg.tags = tags
full_cfg.parameters.extend(build_context_parameters(env, tags))

# COMMAND ----------

full_cfg

# COMMAND ----------

full_cfg.schema

# COMMAND ----------



# COMMAND ----------

# Deploy pipelines based on deployment.yml, in dev mode, specified by env param

response = autopipeline(env="dev")
response

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run pipeline with python code

# COMMAND ----------

# Run the pipeline by pipeline ID
run_pipeline(
    pipeline_id=response["response"]["pipeline_id"]
)

# COMMAND ----------

# Can be used when the pipeline created has the same name as one previously recreated,
# but note that names are no longer idempotent in Databricks
# run_pipeline_by_name(dbutils=dbutils, 
#    pipeline_name=response["pipeline_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tasks for later
# MAGIC ### Task: Deploy to prod

# COMMAND ----------

# import os
# os.environ["DEPLOYMENT_ENV"] = "prod"
# # Deploy pipelines based on deployment.yml, in dev mode
# prod_response = autopipeline(env="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Run prod pipeline with python

# COMMAND ----------

# run_pipeline(
#     dbutils=dbutils, 
#     pipeline_id=prod_response["response"]["pipeline_id"]
# )

# COMMAND ----------


