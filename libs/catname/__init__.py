import inspect
import re


def catname_from_path():
    """Derive catalog name from repo data mesh structure, e.g.
    /Repos/paal@foobar.foo/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    dbutils = inspect.stack()[1][0].f_globals["dbutils"]
    nb_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    print(f"nb_path: {nb_path}")
    ret = re.search(
        "\/Repos\/.+\/orgs\/([^/]+)\/domains\/([^/]+)\/projects\/([^/]+)\/flows\/([^/]+)\/.+",
        nb_path,
        re.IGNORECASE,
    )
    org = ret[1]
    domain = ret[2]
    project = ret[3]
    # flow = ret[4]  # we discard flow for now
    catalog = f"{org}_{domain}_{project}"
    return catalog
