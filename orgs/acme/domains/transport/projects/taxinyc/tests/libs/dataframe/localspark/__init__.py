"""Spark wrapper functions to enable abstractions and testing.
"""
import pyspark
from pyspark.sql import SparkSession
import pytest


__all__ = ['local_session', 'session', 'default_server_session']


def local_session(*, app_name='birgitta_spark_test'):
    """Get a local spark session. Used for recipe tests,
    both running them, and creating fixtures."""
    conf = local_conf_spark()
    # Sets the Spark master URL to connect to, such as:
    #
    #   "local" to run locally,
    #   "local[4]" to run locally with 4 cores,
    #   local[*] Run Spark locally with as many worker threads as logical cores
    #   on your machine,
    #   "spark://89.9.250.25:7077" or "spark://master:7077" to run on a Spark
    #   standalone cluster.
    master_spark_url = 'local[*]'
    session = (SparkSession.builder
               .config(conf=conf)
               .master(master_spark_url)
               .appName(app_name)
               .getOrCreate())
    return session


def local_conf_spark():
    """Configure local spark to be fast for recipe tests."""
    # Speed up config for small test data sets
    conf = pyspark.SparkConf().setAll([
        # No parallelism needed in small data
        ('spark.sql.shuffle.partitions', 1),
        # bindAddress needed to avoid error: "Service 'sparkDriver' could
        # not bind on a random free port. You may check whether
        # configuring an appropriate binding address."
        ('spark.driver.bindAddress', '127.0.0.1'),
        # ('spark.driver.port', 55201),
        # Default time zone to UTC, for consistency
        ("spark.sql.session.timeZone", "UTC")
    ])
    return conf


@pytest.fixture()
def spark_session():
    return local_session()  # duration: about 3secs
