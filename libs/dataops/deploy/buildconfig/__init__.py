from libs.dataops.deploy.defaultconfig import defaultconfig
from libs.dataops.deploy.buildconfig.enrichtasks import enrich_tasks
from libs.dataops.deploy.buildconfig.clusters import add_clusters
from libs.username import databricks_email


def buildconfig(*, cfg, job_name, depname, env, dbutils):
    """Combine custom parameters with default parameters, and default cluster config"""
    full_cfg = defaultconfig().copy()
    full_cfg.update(cfg)
    full_cfg["name"] = job_name
    full_cfg["tags"] = _tags(cfg=cfg, depname=depname, env=env)
    full_cfg = enrich_tasks(cfg=full_cfg, env=env, dbutils=dbutils)
    # Get list of clusters used by tasks
    used_clusters = full_cfg.pop("_used_clusters", {})
    full_cfg = add_clusters(cfg=full_cfg, used_clusters=used_clusters, env=env)
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
