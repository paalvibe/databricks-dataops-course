# Databricks notebook source
import dlt
from pyspark.sql import functions as F


# @dlt.table
# def trips():
#     return spark.table("training.taxinyc_trips.yellow_taxi_trips_curated_sample")


@dlt.table(
    name="revenue_by_borough",
    comment="Aggregated revenue by pickup borough",
    table_properties={"quality": "silver"}
)
def revenue_by_borough():
    return (
        dlt.read("trips").groupBy("pickup_borough")
        .agg(F.sum("total_amount").alias("amount"))
        .sort(F.col("pickup_borough").asc())
    )

