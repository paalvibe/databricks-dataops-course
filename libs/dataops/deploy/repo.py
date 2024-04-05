import requests
from libs.dataops.deploy import api
from libs.dataops.deploy.nbpath import nbrepopath


def git_source(dbutils):
    api_token = api.api_token(dbutils)
    api_host = api.api_host(dbutils)
    _nbrepopath = nbrepopath(dbutils)
    repo_dir_data = _get_status(
        api_token=api_token, api_host=api_host, _nbrepopath=_nbrepopath
    )
    print("repo_dir_data: " + repr(repo_dir_data))
    repo_id = repo_dir_data["object_id"]
    repo = _get_repo(api_token=api_token, api_host=api_host, repo_id=repo_id)
    print("repo: " + repr(repo))
    return {
        "git_url": repo["url"],
        "git_provider": repo["provider"],
        "git_branch": repo["branch"],
        "git_commit": repo["head_commit_id"],
    }


def _get_status(*, api_token, api_host, _nbrepopath):
    """_nbrepopath: path of the repo root folder in the notebook file system"""
    return requests.get(
        f"{api_host}/api/2.0/workspace/get-status",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"path": _nbrepopath},
    ).json()


def _get_repo(*, api_token, api_host, repo_id):
    return requests.get(
        f"{api_host}/api/2.0/repos/{repo_id}",
        headers={"Authorization": f"Bearer {api_token}"},
    ).json()
