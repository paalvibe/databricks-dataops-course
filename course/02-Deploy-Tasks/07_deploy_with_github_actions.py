# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy job(s) through Github Actions.
# MAGIC This can be a bit of a difficult task as there are several moving parts.
# MAGIC Normally this is setup once by your databricks admin,
# MAGIC but having a workshop where 20 people deploy and work on the same repos not ideal, hence this approach.
# MAGIC Feel free to ask your instructor for help.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task - Figure out how it works, step 1
# MAGIC Go to the deploy folder in the base of the repo. Study the json file called ```deploy.json```
# MAGIC which is a job definition that calls the notebook ```deploy_with_github_actions.py```
# MAGIC with the python function.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task - Figure out how it works, step 2
# MAGIC Go to the .github/workflows folder in the base of the repo. Observer the databricks repos update command.
# MAGIC Study the file. Try to determine what it does, and how.
# MAGIC The run the job with the python function.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Generate your databricks PAT.
# MAGIC You will need a Personal Acccess Token to be able to run the Github Actions workflow.
# MAGIC Generate the PAT in databricks.
# MAGIC Save it as a github actions secret in your repo, make sure you use the name DATABRICKS_TOKEN for the secret.
# MAGIC furthermore, remember to save a secret for the DATABRICKS_HOST, or replace it with the actual host
# MAGIC as the hostname is not really a secret.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Clone the repo again in the Repo folder
# MAGIC Note that this is not inside your workspace.
# MAGIC Go to Workspace -> Repos -> Find your username, and clone the repo in there.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Update the deploy.json file with the paths relevant to your context.
# MAGIC Make sure to commit and push your changes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Update the .github/workflows/databricks.yml file with the paths relevant to your context
# MAGIC Make sure to commit and push your changes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task: Go to where you forked the repo on github.com.
# MAGIC Go to the actions tab, and run the workflow. See if the deploy succeeds.




