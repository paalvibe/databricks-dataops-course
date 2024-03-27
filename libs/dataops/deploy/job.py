import json
import requests


def get_existing_job_id(databricks_host, databricks_token, job_name):
    url = f"{databricks_host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {databricks_token}"}
    response = requests.get(url, headers=headers)
    jobs = response.json().get("jobs", [])

    for job in jobs:
        if job["settings"]["name"] == job_name:
            return job["job_id"]
    return None


def create_or_update(
    databricks_host,
    databricks_token,
    job_config_file,
    branch_name=None,
    release_version=None,
):
    with open(job_config_file, "r") as file:
        job_config = json.load(file)

    if release_version:
        job_config["git_source"]["git_tag"] = release_version

    if branch_name and branch_name != "main":
        job_config["name"] += "_" + branch_name
        del job_config["schedule"]

    job_name = job_config["name"]
    job_id = get_existing_job_id(databricks_host, databricks_token, job_name)

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    if job_id:
        print("Resetting job: " + job_name)
        url = f"{databricks_host}/api/2.1/jobs/reset"
        response = requests.post(
            url, headers=headers, json={"job_id": job_id, "new_settings": job_config}
        )
    else:
        print("Creating job: " + job_name)
        url = f"{databricks_host}/api/2.1/jobs/create"
        response = requests.post(url, headers=headers, json=job_config)

    return response.json()


# TODO
"""
Erstatt manuelt kall med bruk av ApiClient på måten som vist under.
Foreløpig så bruker dette kallet API version 2.0 mens det krever 2.1 for å sende inn flere jobber samtidig. @
Oppdater dette når python-pakken er oppdatert til å bruke 2.1 i stedt.

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi

def list_workspace_items(host, token, workspace_path):
    api_client = ApiClient(host=host, token=token)
    workspace_api = WorkspaceApi(api_client)
    items = workspace_api.list_objects(workspace_path)
    return items

"""
