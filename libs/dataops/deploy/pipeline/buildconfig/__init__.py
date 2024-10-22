from libs.dataops.deploy.pipeline.defaultconfig import defaultconfig
from libs.dataops.deploy.pipeline.buildconfig.enrichpipelines import enrich_pipeline
from libs.dataops.deploy.pipeline.buildconfig.policies import policy_set

# from libs.username import databricks_email


def buildconfig(*, cfg, job_name, depname, env, dbutils):
    """Combine custom parameters with default parameters, and default cluster config"""
    full_cfg = defaultconfig().copy()
    full_cfg.update(cfg)
    full_cfg["name"] = job_name
    tags = _tags(cfg=cfg, depname=depname, env=env)
    full_cfg["tags"] = tags
    full_cfg["parameters"] = [
        {
            "name": "deployment_env",
            "default": env,
        },
        {
            "name": "git_url",
            "default": tags["git_url"],
        },
        {
            "name": "git_branch",
            "default": tags["git_branch"],
        },
        {
            "name": "git_commit",
            "default": tags["git_commit"],
        },
    ]
    cfg["development"] = env == "dev"
    # Add pipeline
    full_cfg = enrich_pipeline(cfg=full_cfg, env=env, dbutils=dbutils)
    # Get clusters entry, which in the case of DLT is a list of policies
    policy_name = full_cfg.pop("policy_name")
    cluster_template = full_cfg.pop("cluster_template")
    full_cfg["clusters"] = policy_set(
        dbutils=dbutils, policy_name=policy_name, template_key=cluster_template
    )
    return full_cfg


def _tags(*, cfg, depname, env):
    return {
        "env": env,
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
    }
