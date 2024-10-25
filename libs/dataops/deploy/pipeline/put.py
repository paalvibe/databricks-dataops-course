import requests
from libs.dataops.deploy.pipeline.get import get_existing_pipeline_id


def put(
    *,
    pipeline_name,
    pipeline_config,
    api_token,
    api_host,
):
    pipeline_id = get_existing_pipeline_id(
        api_host=api_host, api_token=api_token, pipeline_name=pipeline_name
    )

    if pipeline_id:
        response = _update(
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
            api_host=api_host,
            api_token=api_token,
            pipeline_config=pipeline_config,
        )
    else:
        response = _create(
            pipeline_name=pipeline_name,
            api_host=api_host,
            api_token=api_token,
            pipeline_config=pipeline_config,
        )

    response_json = response.json()
    repr_str = repr(response_json)
    print(f"put.put() response: {repr_str}")
    return response_json


def _create(*, pipeline_name, api_host, api_token, pipeline_config):
    print("Creating pipeline: " + pipeline_name)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    url = f"{api_host}/api/2.0/pipelines"
    return requests.post(url, headers=headers, json=pipeline_config)


def _update(*, pipeline_name, pipeline_id, api_host, api_token, pipeline_config):
    print("Resetting pipeline: " + pipeline_name)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    url = f"{api_host}/api/2.0/pipelines"
    return requests.put(
        url,
        headers=headers,
        json={"pipeline_id": pipeline_id, "new_settings": pipeline_config},
    )
