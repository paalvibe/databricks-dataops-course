import pytest
from unittest import mock
from libs.dataops.deploy.autojob import autojob


REPO_STATUS = {"object_id": "foo"}

REPO = {
    "url": "https://github.com/paalvibe/databricks-dataops-course",
    "provider": "gitHub",
    "branch": "feature/gh-345-revenue",
    "head_commit_id": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
}

DBRICKS_USERNAME = "paal-peter.paalson@foo.org"
DEPLOY_NB_PATH = (
    "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy"
)
FULLNBPATH = f"/Repos/{DBRICKS_USERNAME}/databricks-dataops-course/{DEPLOY_NB_PATH}"


CLUSTER_LIST_RESPONSE = [
    {
        "cluster_id": "1234-567890-fooo123",
        "cluster_name": "shared-job-cluster-dev",
    }
]


@mock.patch("libs.dataops.deploy.api.api_token", return_value="abcdefgh")
@mock.patch(
    "libs.dataops.deploy.api.api_host", return_value="https://frankfurt.databricks.com"
)
@mock.patch("libs.dataops.deploy.repo._get_status", return_value=REPO_STATUS)
@mock.patch("libs.dataops.deploy.repo._get_repo", return_value=REPO)
@mock.patch("libs.username.databricks_email", return_value=DBRICKS_USERNAME)
@mock.patch(
    "libs.dataops.deploy.buildconfig.databricks_email", return_value=DBRICKS_USERNAME
)
@mock.patch(
    "libs.dataops.deploy.nbpath.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.jobname.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.buildconfig.clusters._get_clusters",
    return_value=CLUSTER_LIST_RESPONSE,
)
@mock.patch("libs.dataops.deploy.job._get_job", return_value=[])
@mock.patch("libs.dataops.deploy.put._create")
def test_autojob_dev_create(create, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10):
    autojob(
        dbutils="something",
        cfgyaml="./libs/tests/dataops/deploy/mock_data/deployment_dev.yml",
    )
    create_cnt = create.call_count
    assert create_cnt == 1
    create_call = create.call_args_list[0]
    job_name = create_call.kwargs.get("job_name")
    assert (
        job_name
        == "acme_transport_taxinyc_prep_dev_paalpeterpaalson_featuregh345revenue_aaaabbbb"
    )
    job_config = create_call.kwargs.get("job_config")
    print("test_autojob.py:" + repr(37) + ":job_config:" + repr(job_config))
    assert job_config == DEV_EXPECTED_CONFIG


DEV_EXPECTED_CONFIG = {
    "name": "dltrevenue_paaldevibe_manual_test",
    "edition": "ADVANCED",
    "catalog": "acme_transport_taxinyc",
    "target": "dev_paalpeterpaalson_featuregh345revenue_aaaabbbb_dltrevenue",
    "data_sampling": False,
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "policy_id": "123B456789D12345",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        },
        {
            "label": "maintenance",
            "policy_id": "123B456789D12345"
        }
    ],
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "photon": False,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/paalpeterpaalson@example.com/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/revenue"
            }
        }
    ],
    "parameters": [
        {
            "default": "dev",
            "name": "deployment_env",
        },
        {
            "default": "https://github.com/paalvibe/databricks-dataops-course",
            "name": "git_url",
        },
        {
            "default": "feature/gh-345-revenue",
            "name": "git_branch",
        },
        {
            "default": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
            "name": "git_commit",
        },
    ],
    "tags": {
        "deployment": "dev_paalpeterpaalson_featuregh345revenue_aaaabbbb",
        "env": "dev",
        "git_branch": "feature/gh-345-revenue",
        "git_commit": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
        "git_url": "https://github.com/paalvibe/databricks-dataops-course",
    },
}