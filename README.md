# databricks-dataops-course
Course for doing databricks dataops, based on a data mesh monorepo structure

## Preparation

1. All members must get commit access to:
    - the repo from the teacher
    - the databricks training workspace

## Purpose of the course

How can we deploy Databricks data pipelines in a way that is:

- (git-)versioned
- usable
- ordered and sustainable
- enabling decentralized domain ownership for each data domain and team
    - this somehow enables data mesh-like principles
- way of working for exploration, development, staging and production of pipelines

## Repository structure

We will do our tasks in the context of the folder representing the revenue data pipeline or flow:

`orgs/acme/domains/transport/projects/taxinyc/flows/prep/revenue/`

The structure is a proposal, which might have to be adapted in a real world organization.

The structure is:

- org: `acme`
    - domain: `transport`
        - project: `taxinyc`
            - flowtype: `prep` (meaning ETL/data engineering, the alternative is `ml`, for ML work)
                - flow: `revenue` 

The structure will be applied to:

- Data *code*, i.e. the pyspark code herein git
- The database *tables* produced by that code
- The data pipelines being deployed

The purpose of this structure is to have sufficient granularity to enable each department/org, team/domain, project and pipeline, to be kept apart.

You can explore the structure here in Databricks, or more easily [in the repo with a browser](https://github.com/paalvibe/databricks-dataops-course).

## Longer explanation of the repo structure

A longer explanation of the ideas behind the repo structure can be found in the article [Data Platform Urbanism - Sustainable Plans for your Data Work](https://www.linkedin.com/pulse/data-platform-urbanism-sustainable-plans-your-work-p%25C3%25A5l-de-vibe/).

## Dataops libs

For the dataops code, we use the [brickops](https://github.com/brickops/brickops) package from Pypi, to enable a versioned pipeline deployment and way of working. The main logic is under [dataops/deploy](https://github.com/brickops/brickops/blob/main/brickops/dataops/deploy/autojob.py).

## Reusing the structure and dataops libs

The structure and brickops libs can be used in your own projects, by forking the repo or copying the content and adapting it.

## Course

1. Go to course/
   - DO NOT run anything under course/00-Workshop-Admin-Prep
2. Got to course/01-Student-Prep/01-General
   - Go through the instructions under that folder
3. Start with the tasks under 02-DeployTasks
   - Some sections are just for reading or running, others you need to solve.