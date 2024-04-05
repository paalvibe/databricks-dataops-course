import json


def api_host(dbutils):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_host = ctx.apiUrl().get()
    return api_host


def api_token(dbutils):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return ctx.apiToken().get()
