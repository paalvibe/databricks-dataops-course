# Databricks notebook source
import dlt
from pyspark.sql import functions as F


@dlt.table
def trips():
    return spark.table("training.taxinyc_trips.yellow_taxi_trips_curated_sample")

