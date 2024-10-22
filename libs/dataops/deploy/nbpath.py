import os.path


def nbrelpath(dbutils):
    """Get relative path of deploy notebook in repo, from the full notebook path

    Example path:
    orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy
    """
    _nbpath = nbpath(dbutils)
    relpath = "/".join(_nbpath.split("/")[4:])
    return relpath


def nbrepopath(dbutils):
    """Get path of repo root, from the full notebook path

    Example path:
    /Repos/foo@bar.org/databricks-dataops-course
    """
    _nbpath = nbpath(dbutils)
    relpath = "/".join(_nbpath.split("/")[:4])
    return relpath


def nbabsfolder(dbutils):
    """Get full file system path of the folder of the
    deploy notebook in repo

    Example path:
    /Repos/foo@bar.org/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue
    """
    _nbpath = nbpath(dbutils)
    # chip off notebook name, and return its folder
    relpathfolder = os.path.dirname(_nbpath)
    print(f"relpathfolder {relpathfolder}")
    return relpathfolder


def nbrelfolder(dbutils):
    """Return relative path of the folder of the notebook

    Example path:
    orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue
    """
    path = nbrelpath(dbutils)
    print(f"path {path}")
    # chip off notebook name, and return its folder
    relpathfolder = os.path.dirname(path)
    print(f"relpathfolder {relpathfolder}")
    return relpathfolder


def nbpath(dbutils):
    """Get full file  system path of deploy notebook in repo

    Example path:
    /Repos/foo@bar.org/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return ctx.notebookPath().get()
