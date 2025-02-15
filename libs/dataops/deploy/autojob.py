# import requests
import inspect
import json
from libs.dataops.deploy.readconfig import read_config_yaml
from libs.dataops.deploy.repo import git_source
from libs.dataops.deploy import api
from libs.dataops.deploy.depname import depname as _depname
from libs.dataops.deploy.job.put import put
from libs.dataops.deploy.job.jobname import jobname
from libs.dataops.deploy.job.buildconfig import buildconfig


def autojob(*, dbutils=None, cfgyaml="deployment.yml", env="dev"):
    """Deploy a job defined in ./deployment.yml.
    Job naming and the rest of the configuration is derived from the environment."""
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    cfg = read_config_yaml(cfgyaml)
    print(
        f"""### Setup job for env {env} ###
    """
    )
    if not dbutils:
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    api_token = api.api_token(dbutils)
    api_host = api.api_host(dbutils)
    cfg["git_source"] = git_source(dbutils)
    depname = _depname(dbutils=dbutils, env=env, git_src=cfg["git_source"])
    job_name = jobname(dbutils=dbutils, depname=depname)
    print(f"""deployment: {depname}""")
    job_config = buildconfig(
        job_name=job_name, cfg=cfg, depname=depname, env=env, dbutils=dbutils
    )
    print("\njob_config:\n" + json.dumps(job_config, sort_keys=True, indent=4))
    print("")
    response = put(
        job_name=job_name, job_config=job_config, api_token=api_token, api_host=api_host
    )
    print("Job deploy finished.")
    return {"job_name": job_name, "response": response}
