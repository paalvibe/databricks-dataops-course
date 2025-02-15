import pytest
from unittest import mock
from unittest.mock import MagicMock
from libs.dataops.deploy.autopipeline import autopipeline


REPO_STATUS = {"object_id": "foo"}

REPO = {
    "url": "https://github.com/paalvibe/databricks-dataops-course",
    "provider": "gitHub",
    "branch": "feature/gh-345-revenue",
    "head_commit_id": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
}

DBRICKS_USERNAME = "paal-peter.paalson@foo.org"
DEPLOY_NB_PATH = (
    "orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/deploy"
)
FULLNBPATH = f"/Repos/{DBRICKS_USERNAME}/databricks-dataops-course/{DEPLOY_NB_PATH}"


POLICIES_LIST_RESPONSE = [
    {
        "policy_id": "123B456789D12345",
        "policy_name": "default_dlt_policy",
    }
]


@mock.patch("libs.dataops.deploy.api.api_token", return_value="abcdefgh")
@mock.patch(
    "libs.dataops.deploy.api.api_host", return_value="https://frankfurt.databricks.com"
)
@mock.patch("libs.catname._nbpath", return_value=FULLNBPATH)
@mock.patch("libs.dataops.deploy.repo._get_status", return_value=REPO_STATUS)
@mock.patch("libs.dataops.deploy.repo._get_repo", return_value=REPO)
@mock.patch("libs.username.databricks_email", return_value=DBRICKS_USERNAME)
@mock.patch(
    "libs.dataops.deploy.nbpath.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.pipeline.pipelinename.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.pipeline.buildconfig.policies._get_policies",
    return_value=POLICIES_LIST_RESPONSE,
)
@mock.patch("libs.dataops.deploy.pipeline.get._get_pipelines", return_value=[])
@mock.patch("libs.dataops.deploy.pipeline.put._create")
def test_autopipeline_dev_create(create, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10):
    autopipeline(
        dbutils=MagicMock(),
        cfgyaml="./libs/tests/dataops/deploy/mock_data/pipeline/deployment_dev.yml",
    )
    create_cnt = create.call_count
    assert create_cnt == 1
    create_call = create.call_args_list[0]
    pipeline_name = create_call.kwargs.get("pipeline_name")
    assert (
        pipeline_name
        == "acme_transport_taxinyc_prep_dev_paalpeterpaalson_featuregh345revenue_aaaabbbb"
    )
    pipeline_config = create_call.kwargs.get("pipeline_config")
    print(
        "test_autopipeline.py:" + repr(37) + ":pipeline_config:" + repr(pipeline_config)
    )
    assert pipeline_config == DEV_EXPECTED_CONFIG


DEV_EXPECTED_CONFIG = {
    "name": "acme_transport_taxinyc_prep_dev_paalpeterpaalson_featuregh345revenue_aaaabbbb",
    "edition": "ADVANCED",
    "catalog": "acme_transport_taxinyc",
    "schema": "dev_paalpeterpaalson_featuregh345revenue_aaaabbbb_dltrevenue",
    "data_sampling": False,
    "pipeline_type": "WORKSPACE",
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "photon": False,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/paal-peter.paalson@foo.org/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/revenue"
            }
        }
    ],
    "serverless": True,
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
