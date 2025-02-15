import requests
from libs.dataops.deploy import api


def policy_set(*, dbutils, policy_name, template_key):
    policy_id = _lookup_policy_id(dbutils=dbutils, policy_name=policy_name)
    return _cluster_templates(template_key=template_key, policy_id=policy_id)


def _lookup_policy_id(*, dbutils, policy_name):
    policy_list = _get_policies(dbutils)
    for policy in policy_list:
        if policy["name"] == policy_name:
            return policy["policy_id"]
    raise Exception(f"Policy {policy_name} not found")


def _get_policies(dbutils):
    api_host = api.api_host(dbutils)
    api_token = api.api_token(dbutils)
    global policy_list
    if policy_list:
        return policy_list
    ret = requests.get(
        f"{api_host}/api/2.0/policies/clusters/list",
        headers={"Authorization": f"Bearer {api_token}"},
    ).json()
    policy_list = ret["policies"]
    return policy_list


def _cluster_templates(*, template_key, policy_id):
    """Templates for different policy types."""
    return {
        "common_dlt": [
            {
                "label": "default",
                "policy_id": policy_id,
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5,
                    "mode": "ENHANCED"
                }
            },
            {
                "label": "maintenance",
                "policy_id": policy_id
            }
        ]
    }
