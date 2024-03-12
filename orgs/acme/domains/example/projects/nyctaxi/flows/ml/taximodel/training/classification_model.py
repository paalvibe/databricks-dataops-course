# Databricks notebook source
import mlflow
import sklearn.model_selection
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data

# COMMAND ----------

df = table("nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC Sample 1% of dataset

# COMMAND ----------

df_s = df.sample(0.01)
print(f"Sample row count: {df_s.count()}")
df_p_s = df_s.toPandas()
print(f"Pandas dataframe shape: {df_p_s.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split data

# COMMAND ----------

li_drop_for_x = ["id", "trip_length", "trip_start"]

df_y = df_p_s[["trip_start"]]
df_x = df_p_s.drop(columns=li_drop_for_x)

print(f"Y dataframe shape: {df_y.shape}")
print(f"X dataframe shape: {df_x.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelling

# COMMAND ----------

# MAGIC %md Set up MLflow

# COMMAND ----------

mlflow.set_experiment(experiment_id=131202974386451)
mlflow.autolog()
mlflow.xgboost.autolog()

# COMMAND ----------

# MAGIC %md Split dataset in training and testing

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    df_x, df_y, test_size=0.20, random_state=42
)

# COMMAND ----------

with mlflow.start_run(run_name='XGBoost') as run:
    model = XGBClassifier(random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    proba = model.predict_proba(X_test)
