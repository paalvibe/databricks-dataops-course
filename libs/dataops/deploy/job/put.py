import requests
from libs.dataops.deploy.job.get import get_existing_job_id


def put(
    *,
    job_name,
    job_config,
    api_token,
    api_host,
):
    job_id = get_existing_job_id(
        api_host=api_host, api_token=api_token, job_name=job_name
    )

    if job_id:
        response = _update(
            job_name=job_name,
            job_id=job_id,
            api_host=api_host,
            api_token=api_token,
            job_config=job_config,
        )
    else:
        response = _create(
            job_name=job_name,
            api_host=api_host,
            api_token=api_token,
            job_config=job_config,
        )

    response_json = response.json()
    repr_str = repr(response_json)
    print(f"put.put() response: {repr_str}")
    return response_json


def _create(*, job_name, api_host, api_token, job_config):
    print("Creating job: " + job_name)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    url = f"{api_host}/api/2.1/jobs/create"
    return requests.post(url, headers=headers, json=job_config)


def _update(*, job_name, job_id, api_host, api_token, job_config):
    print("Resetting job: " + job_name)
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    url = f"{api_host}/api/2.1/jobs/reset"
    return requests.post(
        url, headers=headers, json={"job_id": job_id, "new_settings": job_config}
    )
