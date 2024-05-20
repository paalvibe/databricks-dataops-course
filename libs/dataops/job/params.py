def job_param(*, dbutils, param):
    return dbutils.widgets.get(param)
