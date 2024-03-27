# Flows

Flows ar simple or complex workflows/pipelines to be deployed as Databricks jobs.

## Flow types

Some examples:

An data transformation notebook
A notebook training or evaluating MLFlow-models
A DLT-pipeline notebook
A python-script
A python-wheel
A jar-file
An Airflow DAG

The limitations are those that are supported by Databricks'
[Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html).

## Folder structure

There is an example structure separating between data prep (Data Engineering, ETL),
and ML (training and validation).

Each Flow contains a deployment.yml file pointing to the entry execution file,
and som parameters which databricks jobs can handle.
You need to setup your CICD, with DBX, terraform or similar to
automate the deployment of the deployment defined in deployment.yml.

In each flow folder there can also be an src-folder where you can put
source code called by the entry file.
