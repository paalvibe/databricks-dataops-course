import os
import pytest
from unittest import mock
from libs.tblname import tblname


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
    "libs.dataops.deploy.job.buildconfig.databricks_email", return_value=DBRICKS_USERNAME
)
@mock.patch(
    "libs.dataops.deploy.nbpath.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.job.jobname.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.job.buildconfig.clusters._get_clusters",
    return_value=CLUSTER_LIST_RESPONSE,
)
@mock.patch("libs.dataops.deploy.job.get._get_jobs", return_value=[])
@mock.patch("libs.dataops.deploy.job.put._create")
def test_tblname(create, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10):
    name = tblname(
        tbl="trips", db="nyc_workshop", cat="training", env="dev", dbutils="something"
    )
    assert (
        name
        == "training.dev_paalpeterpaalson_featuregh345revenue_aaaabbbb_nyc_workshop.trips"
    )

    os.environ["DEPLOYMENT_ENV"] = "prod"
    prodname = tblname(
        tbl="trips", db="nyc_workshop", cat="training", dbutils="something"
    )
    del os.environ["DEPLOYMENT_ENV"]
    assert prodname == "training.nyc_workshop.trips"
