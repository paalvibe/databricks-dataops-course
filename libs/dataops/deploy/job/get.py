import requests


def get_existing_job_id(*, api_host, api_token, job_name):
    jobs = _get_jobs(api_host=api_host, api_token=api_token)
    for job in jobs:
        if job["settings"]["name"] == job_name:
            return job["job_id"]
    return None


def _get_jobs(*, api_host, api_token):
    url = f"{api_host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers)
    return response.json().get("jobs", [])

# # TODO
# """
# Erstatt manuelt kall med bruk av ApiClient på måten som vist under.
# Foreløpig så bruker dette kallet API version 2.0 mens det krever 2.1 for å sende inn flere jobber samtidig. @
# Oppdater dette når python-pakken er oppdatert til å bruke 2.1 i stedt.

# from databricks_cli.sdk.api_client import ApiClient
# from databricks_cli.workspace.api import WorkspaceApi

# def list_workspace_items(host, token, workspace_path):
#     api_client = ApiClient(host=host, token=token)
#     workspace_api = WorkspaceApi(api_client)
#     items = workspace_api.list_objects(workspace_path)
#     return items

# """
