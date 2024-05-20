from libs.dataops.deploy.nbpath import nbrelfolder
from libs.dataops.deploy.buildconfig.clusters import env_cluster_key, lookup_cluster_id


def enrich_tasks(*, cfg, dbutils, env):
    tasks = cfg["tasks"]
    enriched_tasks = []
    base_nb_path = nbrelfolder(dbutils)
    used_clusters = {}
    for task in tasks:
        new_task = task.copy()
        if "notebook_task" not in task:
            task_key = task["task_key"]
            new_task["notebook_task"] = {
                "notebook_path": f"{base_nb_path}/{task_key}",
                "source": "GIT",
            }
            # Either a job_cluster_key or an existing_cluster_id
            if "job_cluster_key" in task:
                env_cluster = env_cluster_key(
                    cluster_key=new_task["job_cluster_key"], env=env
                )
                used_clusters[new_task["job_cluster_key"]] = {
                    "env_cluster_key": env_cluster
                }
                new_task["job_cluster_key"] = env_cluster
            elif "existing_cluster_name" in task:
                # Ensure we have a cluster reference
                existing_cluster_name = new_task.pop("existing_cluster_name")
                existing_cluster_name = env_cluster_key(
                    cluster_key=existing_cluster_name, env=env
                )
                new_task["existing_cluster_id"] = lookup_cluster_id(
                    dbutils=dbutils, cluster_name=existing_cluster_name
                )
            else:
                # Ensure we have a cluster reference
                assert "existing_cluster_id" in new_task
            enriched_tasks.append(new_task)
    cfg["_used_clusters"] = used_clusters
    cfg["tasks"] = enriched_tasks
    return cfg
