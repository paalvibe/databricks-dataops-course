import requests
from libs.dataops.deploy import api


def add_clusters(*, cfg, used_clusters, env):
    """Add clusters used by tasks as job_clusters entry in job config"""
    clusters = []
    for template_key in used_clusters.keys():
        env_cluster_key = used_clusters[template_key]["env_cluster_key"]
        cluster = _cluster(template_key=template_key, key=env_cluster_key)
        clusters.append(cluster)
    cfg["job_clusters"] = clusters
    return cfg


def lookup_cluster_id(*, dbutils, cluster_name):
    cluster_list = _get_clusters(dbutils)
    for cluster in cluster_list:
        if cluster["cluster_name"] == cluster_name:
            return cluster["cluster_id"]
    raise Exception(f"Cluster {cluster_name} not found")


cluster_list = None


def _get_clusters(dbutils):
    api_host = api.api_host(dbutils)
    api_token = api.api_token(dbutils)
    global cluster_list
    if cluster_list:
        return cluster_list
    ret = requests.get(
        f"{api_host}/api/2.0/clusters/list",
        headers={"Authorization": f"Bearer {api_token}"},
    ).json()
    cluster_list = ret["clusters"]
    return cluster_list


def _cluster(*, template_key, key):
    templates = cluster_templates()
    cluster = templates[template_key]
    cluster["job_cluster_key"] = key
    return cluster


def cluster_templates():
    """Templates for different cluster types"""
    return {
        "common-job-cluster": {
            "new_cluster": {
                "job_cluster_key": False,
                "cluster_name": "",
                "spark_version": "14.1.x-scala2.12",
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "eu-central-1a",
                },
                "node_type_id": "i3.xlarge",
                "enable_elastic_disk": False,
                "data_security_mode": "SINGLE_USER",
                "num_workers": 1,
            }
        }
    }


# def env_postfix_cluster_keys(*, cfg, env):
#     clusters = cfg["job_clusters"]
#     prefixed_clusters = []
#     for cluster in clusters:
#         new_cluster = cluster.clone()
#         new_cluster["job_cluster_key"] = env_cluster_key(
#             cluster_key=new_cluster["job_cluster_key"], env=env
#         )
#         prefixed_clusters.append(new_cluster)
#     cfg["job_clusters"] = prefixed_clusters
#     return cfg


def env_cluster_key(*, cluster_key, env):
    return f"{cluster_key}-{env}"
