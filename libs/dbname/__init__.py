# dbutils must be passed in as param as databricks lib not available in UC cluster
# from databricks.sdk.runtime import dbutils
import inspect
import re
from libs.username import username
from libs.dataops.deploy.repo import git_source
from libs.dataops.deploy.pipelineenv import pipelineenv


def dbname(
    *,
    db="nyc_workshop",
    cat="training",
    env="dev",
    dbutils=None,
):
    if not dbutils:
        # Get dbutils from calling module, as databricks lib not available in UC cluster
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    if not db:
        raise ValueError("db must be a non-empty string")
    db_prefix = dbprefix(env=env, dbutils=dbutils)
    return f"{cat}.{db_prefix}{db}"
    # ignore catalog for now, until UC is enabled
    # return f"{db_prefix}{db}"


def dbprefix(*, env, dbutils):
    jobparamfoo = dbutils.widgets.get("jobparamfoo")
    print(f"autojob: jobparamfoo: {jobparamfoo}")
    return _depname(dbutils=dbutils, env=env)


def _depname(*, dbutils, env):
    """Compose deployment prefix from env and git config"""
    pipeline_env = pipelineenv(dbutils)
    if pipeline_env == "prod":
        return ""
    dep_prefix = env
    # Only include username in `dev`, we don't want it in `staging`
    if env == "dev":
        uname = username(dbutils)
        dep_prefix = f"{env}_{uname}_"
    # Get git state
    try:
        git_src = git_source(dbutils)
    except Exception:
        # return dep_prefix, if no access to git state
        return dep_prefix

    branch = _clean_branch(git_src["git_branch"])
    commit_shortref = _commit_shortref(git_src["git_commit"])
    return f"{dep_prefix}{branch}_{commit_shortref}_"


def _clean_branch(branch):
    """Strip anything but alphanum from branch name"""
    return re.sub("[\W_]+", "", branch)


def _commit_shortref(commit):
    return commit[:8]
