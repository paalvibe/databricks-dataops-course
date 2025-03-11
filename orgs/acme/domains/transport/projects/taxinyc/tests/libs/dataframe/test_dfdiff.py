import pytest  # noqa F401
from brickops.dataframe import dfdiff
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from .datasets import fixtures, expected  # noqa F


def test_equal(fixtures, expected):  # noqa F811
    assert not dfdiff.diff(actual=fixtures, expected=expected)


def test_error_on_short(fixtures, expected):  # noqa F811
    expected_short = expected.limit(5)
    expected_ret = {
        'cols': None,
        'counts': {'expected': 5,
                   'actual': 10},
        'dtypes': None,
        'rows': 'NOT_EVALUATED',
        'status': 'COUNT_DIFF'
    }
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_short)
    assert ret == expected_ret


def test_error_on_extra_col(fixtures, expected):  # noqa F811
    expected_extra_col = expected.withColumn("foo", F.lit("bar"))
    expected_ret = {
        'cols': {'actual_cols': 'id,letter,number',
                 'expected_cols': 'id,letter,number,foo'},
        'counts': None,
        'dtypes': None,
        'rows': 'NOT_EVALUATED',
        'status': 'COL_DIFF'
    }
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_extra_col)
    assert ret == expected_ret


def test_error_on_col_name_diff(fixtures, expected):  # noqa F811
    expected_col_name_diff = expected.withColumn(
        'numfoo', F.col('number')
    ).drop('number')
    expected_ret = {
        'cols': {'actual_cols': 'id,letter,number',
                 'expected_cols': 'id,letter,numfoo'},
        'counts': None,
        'dtypes': None,
        'rows': 'NOT_EVALUATED',
        'status': 'COL_DIFF'
    }
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_col_name_diff)
    assert ret == expected_ret


def test_error_on_val_diff(fixtures, expected):  # noqa F811
    expected_val_diff = expected.withColumn(
        'number', F.lit(3).cast(LongType()))
    expected_ret = {
        'cols': None,
        'counts': None,
        'dtypes': None,
        'status': 'ROW_DIFF'
    }
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_val_diff)
    rows = ret.pop('rows')
    assert ret == expected_ret
    assert rows['rows_not_in_actual'].count() == 10
    assert rows['rows_not_in_expected'].count() == 10


def test_error_on_single_val_diff(fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 300, 3333).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    expected_ret = {
        'cols': None,
        'counts': None,
        'dtypes': None,
        'status': 'ROW_DIFF'
    }
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_single_val_diff)
    rows = ret.pop('rows')
    assert ret == expected_ret
    assert rows['rows_not_in_actual'].count() == 1
    assert rows['rows_not_in_expected'].count() == 1


def test_exponential_diff_equal(
        fixtures, expected):  # noqa F811
    expected_ret = None
    ret = dfdiff.exponential_diff(
        actual=fixtures,
        expected=expected,
        max_rows=1024,
        base_size=2
    )
    assert ret == expected_ret


def test_exponential_diff_error_on_single_val_diff(
        fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 300, 3333).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    expected_ret = {
        'cols': None,
        'counts': None,
        'dtypes': None,
        'status': 'ROW_DIFF'
    }
    ret = dfdiff.exponential_diff(
        actual=fixtures,
        expected=expected_single_val_diff,
        max_rows=64)
    print("test_dfdiff.py:" + repr(98) + ":ret:" + repr(ret))
    rows = ret.pop('rows')
    assert ret == expected_ret
    assert rows['rows_not_in_actual'].count() == 1
    assert rows['rows_not_in_expected'].count() == 1


def test_exponential_diff_max_rows(
        fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 900, 9999).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    expected_ret = {
        'cols': None,
        'counts': None,
        'dtypes': None,
        'status': 'NO_ROW_DIFF_YET',
        'rows': 'NO_ROW_DIFF_YET'
    }
    ret = dfdiff.exponential_diff(
        actual=fixtures,
        expected=expected_single_val_diff,
        base_size=2,
        max_rows=4)
    assert ret == expected_ret


def test_print_val_diff(fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 900, 9999).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_single_val_diff)
    rows = ret.pop('rows')
    dfdiff.print_row_sets(
        actual=rows['rows_not_in_expected'],
        expected=rows['rows_not_in_actual']
    )


def test_panda_diff_rows_val_diff(fixtures, expected):  # noqa F811
    num_when = F.when(F.col('number') == 900, 9999).otherwise(F.col('number'))
    expected_single_val_diff = expected.withColumn('number', num_when)
    ret = dfdiff.diff(
        actual=fixtures,
        expected=expected_single_val_diff)
    rows = ret.pop('rows')
    dfdiff.panda_diff_rows(
        actual=rows['rows_not_in_expected'],
        expected=rows['rows_not_in_actual']
    )
