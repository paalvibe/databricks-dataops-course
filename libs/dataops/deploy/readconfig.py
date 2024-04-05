import json
import yaml
from pathlib import Path


def read_config_yaml(cfgfile):
    ret = None
    with open(cfgfile) as stream:
        ret = yaml.safe_load(stream)
    return ret


def read_config_json(cfgfile):
    with open(cfgfile, "r") as file:
        job_config = json.load(file)
    return job_config
