import inspect
import re


def catname_from_path(dbutils=None):
    """Derive catalog name from repo data mesh structure, e.g.
    /Repos/paal@foobar.foo/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    if not dbutils:
        dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    nb_path = _nbpath(dbutils)
    print(f"nb_path: {nb_path}")
    path_re = _select_path_re(nb_path)
    ret = re.search(
        path_re,
        nb_path,
        re.IGNORECASE,
    )
    org = ret[1]
    domain = ret[2]
    project = ret[3]
    # flow = ret[4]  # we discard flow for now
    catalog = f"{org}_{domain}_{project}"
    return catalog


def _nbpath(dbutils):
    """e.g. /Repos/paal@foobar.foo/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue"""
    return (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )


def _select_path_re(nb_path):
    """For now use an optimistic assumption that we don't need to match on first part of path, e.g.:

    /Workspace/Repos/foo@example.com/databricks-dataops-course/

    or

    /Workspace/Users/foo@example.com/Repos/databricks-dataops-course/

    Databricks has gradually changed default paths, so we need a more tolerant first part of the path.
    """
    return ".+\/orgs\/([^/]+)\/domains\/([^/]+)\/projects\/([^/]+)\/flows\/([^/]+)\/.+"
