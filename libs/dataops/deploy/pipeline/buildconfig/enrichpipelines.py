from libs.dataops.deploy.nbpath import nbabsfolder
from libs.catname import catname_from_path
from libs.dbname import dbname


def enrich_pipeline(*, cfg, dbutils, env):
    new_pipeline = cfg.pop("pipeline_tasks")[0]
    print("enrichpipelines.py:" + repr(8) + ":cfg.pop:" + repr(cfg.pop))
    print("enrichpipelines.py:" + repr(8) + ":new_pipeline:" + repr(new_pipeline))
    # Set target catalog
    cat = catname_from_path(dbutils)
    db = new_pipeline.pop("db")
    cfg["catalog"] = cat
    # Set target database/schema
    pipeline_key = new_pipeline["pipeline_key"]
    cfg["schema"] = dbname(cat=cat, db=db, env=env, dbutils=dbutils, prepend_cat=False)
    cfg["development"] = env == "dev"
    # For now, dlt does not support gitrefs, so we must use absolute path
    base_nb_path = nbabsfolder(dbutils)
    cfg["libraries"] = [
        {
            "notebook": {
                "path": f"{base_nb_path}/{pipeline_key}",
            }
        }
    ]
    return cfg
