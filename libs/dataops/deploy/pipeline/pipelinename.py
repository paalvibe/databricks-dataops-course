import re
from libs.dataops.deploy.nbpath import nbpath


def pipelinename(dbutils, depname):
    _nbpath = nbpath(dbutils)
    # Example path: "/Repos/foo@bar.org/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/deploy"
    ret = re.search(
        ".+\/orgs\/([^/]+)\/domains\/([^/]+)\/projects\/([^/]+)\/flows\/([^/]+)\/.+",
        _nbpath,
        re.IGNORECASE,
    )
    print(f"Derive pipelinename from path {_nbpath}")
    org = ret[1]
    domain = ret[2]
    project = ret[3]
    flow = ret[4]
    print(f"""
## Pipeline ##
          
org: {org}, domain: {domain}, project: {project}, flow (pipeline): {flow}""")
    name = f"{org}_{domain}_{project}_{flow}_{depname}"
    return name
