import pytest
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from tests.libs.dataframe import localspark


fixtures_schema = StructType([
    StructField('id', LongType()),
    StructField('letter', StringType()),
    StructField('number', LongType())
])


fixtures_data = [
    [1, 'a', 100],
    [2, 'b', 200],
    [3, 'c', 300],
    [4, 'd', 400],
    [5, 'e', 500],
    [6, 'f', 600],
    [7, 'g', 700],
    [8, 'h', 800],
    [9, 'i', 900],
    [10, 'j', 1000]
]


@pytest.fixture(scope="session")
def spark_session():
    return localspark.local_session()  # duration: about 3secs


@pytest.fixture()
def fixtures():
    spark_session = localspark.local_session()
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


@pytest.fixture()
def expected():
    spark_session = localspark.local_session()
    expected = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return expected
