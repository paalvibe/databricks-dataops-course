from libs.dataops.deploy.nbpath import nbabsfolder
from libs.catname import catname_from_path
from libs.dbname import dbname


def enrich_pipeline(*, cfg, dbutils, env):
    new_pipeline = cfg.pop("pipeline_task")
    # Set target catalog
    cat = catname_from_path(dbutils)
    db = cfg.pop("db")
    cfg["catalog"] = cat
    # Set target database/schema
    pipeline_key = new_pipeline["pipeline_key"]
    cfg["target"] = dbname(cat=cat, db=db, env=env, dbutils=dbutils, prepend_cat=False)
    # For now, dlt does not support gitrefs, so we must use absolute path
    base_nb_path = nbabsfolder(dbutils)
    cfg["libraries"] = [
        {
            "notebook": f"{base_nb_path}/{pipeline_key}",
        }
    ]
    return cfg
