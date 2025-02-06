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
    ret = requests.get(
        f"{api_host}/api/2.0/pipelines/list",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"name": pipeline_name},
    ).json()
    print("Get pipelines response:" + repr(ret))
    pipelines = ret["pipelines"]
    return pipelines[0]


def run_pipeline(*, dbutils, pipeline_id):
    """Run pipeline by pipeline_id"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    return requests.post(
        f"{api_host}/api/2.0/pipelines/run-now",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"pipeline_id": pipeline_id},
    ).json()
