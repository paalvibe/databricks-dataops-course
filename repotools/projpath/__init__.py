import re
import os
import sys


def proj_path():
    """Get path of current projects dir in domenemal structure"""
    cur_path = os.getcwd()
    output = re.search('(/.*/projects/[^/]+)', cur_path, flags=re.IGNORECASE)
    return output[0]

def add_project_dir_to_syspath():
    """Add project path to syspath, to be able to load libs deeper than 4 directories into repo.
    Databricks seem to have 4 as max."""
    p_path = proj_path()
    if p_path in sys.path:
        return
    sys.path.append(os.path.abspath(p_path))
