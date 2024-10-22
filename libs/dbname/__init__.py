# dbutils must be passed in as param as databricks lib not available in UC cluster
# from databricks.sdk.deployment import dbutils
import inspect
import re
from libs.username import username
from libs.dataops.deploy.repo import git_source
from libs.dataops.deploy.deploymentenv import deploymentenv


def dbname(
    *,
    db="nyc_workshop",
    cat="training",
    env="dev",
    dbutils=None,
    prepend_cat=True,
):
    if not dbutils:
        # Get dbutils from calling module, as databricks lib not available in UC cluster
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    if not db:
        raise ValueError("db must be a non-empty string")
    db_prefix = dbprefix(env=env, dbutils=dbutils)
    db_only = f"{db_prefix}{db}"
    if prepend_cat:
        return f"{cat}.{db_only}"
    return db_only
    # ignore catalog for now, until UC is enabled
    # return f"{db_prefix}{db}"


def dbprefix(*, env, dbutils):
    return _depname(dbutils=dbutils, env=env)


def _git_src(dbutils):
    """Get git src params from either task params or repos api.
    Fall back to task params, since repos API might be denied from jobs."""
    try:
        return git_source(dbutils)
    except Exception:
        print("No access to repos API, so use widget params")
    # return dep_prefix, if no access to git state
    return _git_src_from_widget_params(dbutils)


def _git_src_from_widget_params(dbutils):
    return {
        "git_url": dbutils.widgets.get("git_url"),
        "git_branch": dbutils.widgets.get("git_branch"),
        "git_commit": dbutils.widgets.get("git_commit"),
    }


def _depname(*, dbutils, env):
    """Compose deployment prefix from env and git config"""
    deployment_env = deploymentenv(dbutils)
    if deployment_env == "prod":
        return ""
    dep_prefix = env
    # Only include username in `dev`, we don't want it in `staging`
    if env == "dev":
        uname = username(dbutils)
        dep_prefix = f"{env}_{uname}_"
    # Get git state to build db name
    git_src = _git_src(dbutils)
    branch = _clean_branch(git_src["git_branch"])
    commit_shortref = _commit_shortref(git_src["git_commit"])
    return f"{dep_prefix}{branch}_{commit_shortref}_"


def _clean_branch(branch):
    """Strip anything but alphanum from branch name"""
    return re.sub("[\W_]+", "", branch)


def _commit_shortref(commit):
    return commit[:8]
