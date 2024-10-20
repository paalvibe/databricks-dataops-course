import os


def deploymentenv(dbutils):
    """Compose deployment name from env and git config"""
    """If deployment env is set in os.environ or in job tags, return it"""
    os_env = os.environ.get("DEPLOYMENT_ENV", None)
    # Prod should have no prefix
    if os_env:
        return os_env

    widget_deployment_env = _widget_deployment_env(dbutils)
    if widget_deployment_env:
        return widget_deployment_env
    
    return None

    # Could not find a way of getting tags
    # tag_deployment_env = _job_tag(dbutils=dbutils, key="DEPLOYMENT_ENV")
    # # task_param = _task_param(dbutils=dbutils, param="DEPLOYMENT_ENV", dbutils=dbutils)
    # if tag_deployment_env:
    #     return tag_deployment_env
    # return None


def _widget_deployment_env(dbutils):
    """Get a job parameter by referring to it with widgets.get().
    The job parameter is defined in job config."""
    try:
        deployment_env = dbutils.widgets.get("deployment_env")
        print(f"dbprefix: deployment_env: {deployment_env}")
        return deployment_env
    except Exception as e:
        # print("widget deployment_env not accessible")
        return None


# def _job_tag(*, dbutils, key):
#     try:
#         ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#         tags = ctx.tags().get()
#     except Exception:
#         print("tags not available")
#         return None
#     print("deploymentenv.py:" + repr(27) + ":tags:" + repr(tags))
#     if key in tags:
#         return tags[key]
#     return None

    # # context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    # # context = json.loads(context_str)
    # run_id = (
    #     dbutils.notebook.entry_point.getDbutils()
    #     .notebook()
    #     .getContext()
    #     .currentRunId()
    #     .get()
    # )
    # run_id_obj = context.get('currentRunId', {})
    # run_id = run_id_obj.get('id', None) if run_id_obj else None
    # job_id = context.get('tags', {}).get('jobId', None)


# def _task_param(*, dbutils, param, dbutils):
#     context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
# context = json.loads(context_str)
# run_id_obj = context.get('currentRunId', {})
# run_id = run_id_obj.get('id', None) if run_id_obj else None
# job_id = context.get('tags', {}).get('jobId', None)

#     dbutils.jobs.taskValues.get(taskKey    = "task-name-dbx-workflow", \
#                             key        = "variable-name", \
#                             default    = 7, \
#                             debugValue = "debug-value")
