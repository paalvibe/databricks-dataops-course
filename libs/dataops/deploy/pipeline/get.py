import requests


def get_existing_pipeline_id(*, api_host, api_token, pipeline_name):
    pipelines = _get_pipelines(api_host=api_host, api_token=api_token)
    for pipeline in pipelines:
        if pipeline["name"] == pipeline_name:
            return pipeline["pipeline_id"]
    return None


def _get_pipelines(*, api_host, api_token):
    url = f"{api_host}/api/2.0/pipelines/list"
    headers = {"Authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers)
    return response.json().get("pipelines", [])
