def username(dbutils):
    """Return username (an email), stripped for dots and special chars.
    It is used part of naming of dev db prefix, and similar."""
    # must be passed in as param as databricks lib not available in UC cluster
    email = databricks_email(dbutils)
    name = email.split("@")[0].replace(".", "").replace("-", "")
    return name


def databricks_email(dbutils):
    return (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )
