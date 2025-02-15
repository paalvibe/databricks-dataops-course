import requests


def run_pipeline_by_name(*, dbutils, pipeline_name):
    """Run a databricks pipeline by name"""
    pipeline = pipeline_by_name(dbutils=dbutils, pipeline_name=pipeline_name)
    pipeline_id = pipeline["pipeline_id"]
    print(f"pipeline_id: {pipeline_id}")
    return run_pipeline(dbutils=dbutils, pipeline_id=pipeline_id)


def pipeline_by_name(*, dbutils, pipeline_name):
    """Get pipeline by name"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    print(f"pipeline_name: {pipeline_name}")
    ret = requests.get(
        f"{api_host}/api/2.0/pipelines",
        headers={"Authorization": f"Bearer {api_token}"},
        # equals is not supported, so use strict like
        params={"filter": f"name like '{pipeline_name}%'"},
     ).json()
    print("Get pipelines response:" + repr(ret))
    pipelines = ret["statuses"]
    return pipelines[0]


def run_pipeline(*, dbutils, pipeline_id):
    """Run pipeline by pipeline_id"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    return requests.post(
        f"{api_host}/api/2.0/pipelines/{pipeline_id}/updates",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"full_refresh": True}
    ).json()
