# must be passed in as param as databricks lib not available in UC cluster
# from databricks.sdk.runtime import dbutils
import inspect
from libs.dbname import dbname


def tblname(
    *,
    tbl,
    db="nyc_workshop",
    cat="training",
    env="dev",
    dbutils=None,
):
    """cat is the Unity Catalog catalog name"""
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    if not dbutils:
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    if not tbl:
        raise ValueError("tbl must be a non-empty string")
    if not db:
        raise ValueError("db must be a non-empty string")
    db_name = dbname(db=db, cat=cat, env=env, dbutils=dbutils)
    return f"{db_name}.{tbl}"
