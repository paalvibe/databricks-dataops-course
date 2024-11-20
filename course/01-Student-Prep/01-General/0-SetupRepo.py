# Databricks notebook source
# MAGIC %md
# MAGIC # Setup repo
# MAGIC
# MAGIC Either clone the repo or get commit permissions from the repo owner
# MAGIC
# MAGIC
# MAGIC 1. Checkout the repo here on Databricks
# MAGIC     - Make sure you have forked the repo to your own Github account, with the Fork-button.
# MAGIC     - In Databricks, go to Workspace
# MAGIC     - Press `Create` button, and select `Git Folder`. Paste in the path to your fork of the repo https://github.com/foobar123456/databricks-dataops-course. It will automatically fill out the other fields. Press `Create Git folder`.
# MAGIC 2. Setup Databricks/repo to be able to receive commits from Databricks, by approving the Databricks app. and ask you to link Github with Databricks. Accept the Databricks app, but only for this repo (databricks-dataops-course). Then try creating the branch again.
# MAGIC
# MAGIC ![Link Databricks and Github](https://raw.githubusercontent.com/paalvibe/databricks-dataops-course/refs/heads/main/images/link_databricks_github.png)
# MAGIC
# MAGIC 3. [Create your own branch](https://docs.databricks.com/en/repos/git-operations-with-repos.html#create-a-new-branch)
# MAGIC     - Named like such: `feat/gh-[ your initial letters converted to number ]-[ favourite place ]`, but not too long, using the character number [from here](https://www.worldometers.info/languages/how-many-letters-alphabet/).
# MAGIC     - E.g.: `feat-gh[pv]-[place]` => `feat-gh1622-assisi`.
# MAGIC     - Do NOT use your name or username as part of the branch name.  Don't use non-ascii characters in the place name.
# MAGIC
# MAGIC ![Repos button](https://raw.githubusercontent.com/paalvibe/databricks-dataops-course/refs/heads/main/images/switch_branch.png)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


