import requests


def run_job_by_name(*, dbutils, job_name):
    """Run a databricks job by name"""
    job = job_by_name(dbutils=dbutils, job_name=job_name)
    job_id = job["job_id"]
    print(f"job_id: {job_id}")
    return run_job(dbutils=dbutils, job_id=job_id)


def job_by_name(*, dbutils, job_name):
    """Get job by name"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    jobs = requests.get(
        f"{api_host}/api/2.1/jobs/list",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"name": job_name},
    ).json()['jobs']
    return jobs[0]


def run_job(*, dbutils, job_id):
    """Run job by job_id"""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    return requests.post(
        f"{api_host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"job_id": job_id},
    ).json()
