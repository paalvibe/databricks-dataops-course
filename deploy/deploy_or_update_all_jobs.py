# Databricks notebook source
import concurrent.futures
from pathlib import Path

# This is intentionally constrained to my personal git repo, and a specific project for demo purposes
# in the course. In a real-world scenario, you would likely find jobs from multiple domains and projects
# and ensure that a service_principal is used for deployment.

deploy_notebooks = [
    str(path)
    for path in Path("/Workspace/Repos/yourusername/databricks-dataops-course/orgs/acme/domains/transport/projects/").glob(
        "**/deploy"
    )
    if "example_" not in str(path)
]

display(deploy_notebooks)

# COMMAND ----------
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [
        executor.submit(dbutils.notebook.run, str(notebook), timeout_seconds=180)
        for notebook in deploy_notebooks
    ]

    for future in concurrent.futures.as_completed(futures):
        display(future.result())
