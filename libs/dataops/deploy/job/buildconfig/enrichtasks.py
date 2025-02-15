from libs.dataops.deploy.nbpath import nbrelfolder
from libs.dataops.deploy.job.buildconfig.clusters import env_cluster_key, lookup_cluster_id


def enrich_tasks(*, cfg, dbutils, env):
    tasks = cfg["tasks"]
    enriched_tasks = []
    base_nb_path = nbrelfolder(dbutils)
    for task in tasks:
        new_task = task.copy()
        if "notebook_task" not in task:
            task_key = task["task_key"]
            new_task["notebook_task"] = {
                "notebook_path": f"{base_nb_path}/{task_key}",
                "source": "GIT",
            }
        enriched_tasks.append(new_task)

    cfg["tasks"] = enriched_tasks
    return cfg
