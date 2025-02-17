from datetime import date

import pytest
from libs.dataframe import dfdiff
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from tests.libs.dataframe import localspark

from libs.age import gen_age


@pytest.fixture(scope="session")
def spark_session():
    return localspark.local_session()


fixtures_schema = StructType([
    StructField('datecol', DateType())
])


@pytest.fixture()
def fixtures_data():
    return [
        [date(2010, 12, 31)],
        [date(2015, 5, 31)],
        [date(2020, 6, 15)],
        [date(2022, 6, 15)]
    ]


@pytest.fixture()
def fixtures(fixtures_data):
    spark_session = localspark.local_session()
    fixtures = spark_session.createDataFrame(fixtures_data, fixtures_schema)
    return fixtures


expected_schema = StructType([
    StructField('datecol', DateType()),
    StructField('age', LongType()),
])


@pytest.fixture()
def expected_data():
    return [
        [date(2010, 12, 31), 11],
        [date(2015, 5, 31), 7],
        [date(2020, 6, 15), 2],
        [date(2022, 6, 15), 0]
    ]


@pytest.fixture()
def expected(expected_data):
    spark_session = localspark.local_session()
    expected = spark_session.createDataFrame(expected_data, expected_schema)
    return expected


def test_gen_age(fixtures, expected):
    print("fixtures")
    fixtures.show()
    print("expected")
    expected.show()
    today = date(2022, 9, 1)
    w_age = gen_age(df=fixtures, today=today, datecol="datecol")
    print("w_age")
    w_age.show()
    assert not dfdiff.diff(actual=w_age, expected=expected)
