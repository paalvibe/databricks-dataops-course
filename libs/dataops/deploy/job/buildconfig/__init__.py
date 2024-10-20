from libs.dataops.deploy.job.defaultconfig import defaultconfig
from libs.dataops.deploy.job.buildconfig.enrichtasks import enrich_tasks
from libs.dataops.deploy.job.buildconfig.clusters import add_clusters
from libs.username import databricks_email


def buildconfig(*, cfg, job_name, depname, env, dbutils):
    """Combine custom parameters with default parameters, and default cluster config"""
    full_cfg = defaultconfig().copy()
    full_cfg.update(cfg)
    full_cfg["name"] = job_name
    tags = _tags(cfg=cfg, depname=depname, env=env)
    full_cfg["tags"] = tags
    # Parameters are available as widget params
    # e.g. dbutils.widgets.get("deployment_env")
    # Other dbutils functions might be limited by run permissions.
    # Example error:
    # PERMISSION_DENIED: Missing required permissions [View] on node with ID
    # So we use widget parameters instead.
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
    full_cfg = enrich_tasks(cfg=full_cfg, env=env, dbutils=dbutils)
    full_cfg["run_as"] = {
        "user_name": databricks_email(dbutils),
    }

    return full_cfg


def _tags(*, cfg, depname, env):
    return {
        "env": env,
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
    }
