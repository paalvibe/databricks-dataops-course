import re
from libs.username import username


def depname(*, dbutils, env, git_src):
    """Compose deployment name from env and git config"""
    dep_prefix = env
    if env == "dev":
        uname = username(dbutils)
        dep_prefix = f"{env}_{uname}"
    branch = _clean_branch(git_src["git_branch"])
    commit_shortref = _commit_shortref(git_src["git_commit"])
    dep = f"{dep_prefix}_{branch}_{commit_shortref}"
    return dep


def _clean_branch(branch):
    """Strip anything but alphanum from branch name"""
    return re.sub("[\W_]+", "", branch)


def _commit_shortref(commit):
    return commit[:8]
