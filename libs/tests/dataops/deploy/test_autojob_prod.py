import pytest
from unittest import mock
from libs.dataops.deploy.autojob import autojob


REPO_STATUS = {"object_id": "foo"}

REPO = {
    "url": "https://github.com/paalvibe/databricks-dataops-course",
    "provider": "gitHub",
    "branch": "main",
    "head_commit_id": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
}

DBRICKS_USERNAME = "paal-peter.paalson@foo.org"
DEPLOY_NB_PATH = (
    "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy"
)
FULLNBPATH = f"/Repos/{DBRICKS_USERNAME}/databricks-dataops-course/{DEPLOY_NB_PATH}"


@mock.patch("libs.dataops.deploy.api.api_token", return_value="abcdefgh")
@mock.patch(
    "libs.dataops.deploy.api.api_host", return_value="https://frankfurt.databricks.com"
)
@mock.patch("libs.dataops.deploy.repo._get_status", return_value=REPO_STATUS)
@mock.patch("libs.dataops.deploy.repo._get_repo", return_value=REPO)
@mock.patch("libs.username.databricks_email", return_value=DBRICKS_USERNAME)
@mock.patch(
    "libs.dataops.deploy.job.buildconfig.databricks_email",
    return_value=DBRICKS_USERNAME,
)
@mock.patch(
    "libs.dataops.deploy.nbpath.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch(
    "libs.dataops.deploy.job.jobname.nbpath",
    return_value=FULLNBPATH,
)
@mock.patch("libs.dataops.deploy.job.get._get_jobs", return_value=[])
@mock.patch("libs.dataops.deploy.job.put._create")
def test_autojob_prod_create(create, p1, p2, p3, p4, p5, p6, p7, p8, p9):
    autojob(
        dbutils="something",
        cfgyaml="./libs/tests/dataops/deploy/mock_data/job/deployment.yml",
        env="prod",
    )
    create_cnt = create.call_count
    assert create_cnt == 1
    create_call = create.call_args_list[0]
    job_name = create_call.kwargs.get("job_name")
    assert job_name == "acme_transport_taxinyc_prep_prod_main_aaaabbbb"
    job_config = create_call.kwargs.get("job_config")
    print("test_autojob.py:" + repr(37) + ":job_config:" + repr(job_config))
    assert job_config == PROD_EXPECTED_CONFIG


PROD_EXPECTED_CONFIG = {
    "name": "acme_transport_taxinyc_prep_prod_main_aaaabbbb",
    "email_notifications": {
        "no_alert_for_skipped_runs": False,
    },
    "schedule": {
        "quartz_cron_expression": "27 0 16 ? * Thu",
        "timezone_id": "Europe/Brussels",
        "pause_status": "UNPAUSED",
    },
    "git_source": {
        "git_branch": "main",
        "git_commit": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
        "git_provider": "gitHub",
        "git_url": "https://github.com/paalvibe/databricks-dataops-course",
    },
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "job_cluster_key": "common-job-cluster",
            "notebook_task": {
                "notebook_path": "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue_by_borough",
                "source": "GIT",
            },
            "run_if": "ALL_SUCCESS",
            "task_key": "revenue_by_borough",
        },
        {
            "job_cluster_key": "common-job-cluster",
            "notebook_task": {
                "notebook_path": "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue_by_tripmonth",
                "source": "GIT",
            },
            "run_if": "ALL_SUCCESS",
            "task_key": "revenue_by_tripmonth",
        },
        {
            "job_cluster_key": "common-job-cluster",
            "notebook_task": {
                "notebook_path": "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/borough_population",
                "source": "GIT",
            },
            "run_if": "ALL_SUCCESS",
            "task_key": "borough_population",
        },
        {
            "depends_on": [
                {
                    "task_key": "revenue_by_borough",
                },
                {
                    "task_key": "borough_population",
                },
            ],
            "job_cluster_key": "common-job-cluster",
            "notebook_task": {
                "notebook_path": "orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue_by_inhabitant",
                "source": "GIT",
            },
            "run_if": "ALL_SUCCESS",
            "task_key": "revenue_by_inhabitant",
        },
    ],
    "job_clusters": [],
    "parameters": [
        {"default": "prod", "name": "deployment_env"},
        {
            "default": "https://github.com/paalvibe/databricks-dataops-course",
            "name": "git_url",
        },
        {"default": "main", "name": "git_branch"},
        {"default": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111", "name": "git_commit"},
    ],
    "run_as": {
        "user_name": "paal-peter.paalson@foo.org",
    },
    "tags": {
        "deployment": "prod_main_aaaabbbb",
        "env": "prod",
        "git_branch": "main",
        "git_commit": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
        "git_url": "https://github.com/paalvibe/databricks-dataops-course",
    },
}
