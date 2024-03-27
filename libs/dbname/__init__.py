# dbutils must be passed in as param as databricks lib not available in UC cluster
# from databricks.sdk.runtime import dbutils
import inspect


def username(dbutils):
    """Return username, stripped for dots, part of users's email to use as dev db prefix"""
    # must be passed in as param as databricks lib not available in UC cluster
    email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    name = email.split("@")[0].replace(".", "").replace("-", "")
    return name


def dbname(
*, 
db="nyc_workshop",
catalog="training",
env="dev",
):
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    dbutils = inspect.stack()[1][0].f_globals['dbutils']
    if not db:
        raise ValueError("db must be a non-empty string")
    db_prefix = ""
    if env == "dev":
        uname = username(dbutils)
        db_prefix = f"dev_{uname}_"
    return f"{catalog}.{db_prefix}{db}"
    # ignore catalog for now, until UC is enabled
    # return f"{db_prefix}{db}"
