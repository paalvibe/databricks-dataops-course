# import requests
import inspect
import json
from libs.dataops.deploy.readconfig import read_config_yaml
from libs.dataops.deploy.repo import git_source
from libs.dataops.deploy import api
from libs.dataops.deploy.depname import depname as _depname
from libs.dataops.deploy.pipeline.put import put
from libs.dataops.deploy.pipeline.pipelinename import pipelinename
from libs.dataops.deploy.pipeline.buildconfig import buildconfig


def autopipeline(*, dbutils=None, cfgyaml="deployment.yml", env="dev"):
    """Deploy a pipeline defined in ./deployment.yml.
    Pipeline naming and the rest of the configuration is derived from the environment."""
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    cfg = read_config_yaml(cfgyaml)
    print(
        f"""### Setup pipeline for env {env} ###
    """
    )
    if not dbutils:
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    api_token = api.api_token(dbutils)
    api_host = api.api_host(dbutils)
    cfg["git_source"] = git_source(dbutils)
    depname = _depname(dbutils=dbutils, env=env, git_src=cfg["git_source"])
    pipeline_name = pipelinename(dbutils=dbutils, depname=depname)
    print(f"""deployment: {depname}""")
    pipeline_config = buildconfig(
        pipeline_name=pipeline_name, cfg=cfg, depname=depname, env=env, dbutils=dbutils
    )
    print("\npipeline_config:\n" + json.dumps(pipeline_config, sort_keys=True, indent=4))
    print("")
    response = put(
        pipeline_name=pipeline_name, pipeline_config=pipeline_config, api_token=api_token, api_host=api_host
    )
    print("Pipeline deploy finished.")
    return {"pipeline_name": pipeline_name, "response": response}
