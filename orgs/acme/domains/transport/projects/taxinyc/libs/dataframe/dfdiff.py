"""Functions for diffing pyspark dataframes."""

import pandas as pd


__all__ = ['diff', 'schema_diff', 'exponential_diff', 'print_row_sets', 'print_diff_first_rows']


def diff(*, expected, actual):
    """Diff two dataframes."""
    s_diff = schema_diff(expected=expected, actual=actual)
    if s_diff:
        return s_diff
    expected_count = expected.count()
    actual_count = actual.count()
    # Check same number of rows
    if expected_count != actual_count:
        print("""Error: Row count diff
Expected: %s
Actual:   %s""" % (expected_count, actual_count))
        return _diff_ret(status='COUNT_DIFF',
                         counts={
                             'expected': expected_count,
                             'actual': actual_count
                         })
    return _diff_rows(expected=expected, actual=actual)


def schema_diff(*, expected, actual):
    """Check if schema is different"""
    col_diff = _cols_diff(expected, actual)
    if col_diff:
        return _diff_ret(status='COL_DIFF', cols=col_diff)
        return col_diff
    dtype_diff = _dtypes_diff(expected, actual)
    if dtype_diff:
        return _diff_ret(status='DTYPE_DIFF', dtypes=dtype_diff)
    return None


def exponential_diff(*, expected,
                     actual,
                     max_rows=1000000,
                     base_size=16,
                     multiplier=2):
    """Go through data set, in exponential chunks, looking
    for differing records, until max_rows have been examined.
    It is assumed that both datasets are sorted.
    The purpose is to faster find differing records, avoiding a full scan.

    We don't use offset, for now, since not supported easily by spark,
    and performance isn't very different.
    """
    s_diff = schema_diff(expected=expected,
                         actual=actual)
    if s_diff:
        return s_diff
    limit = base_size
    while True:
        print("Look for differing rows in first", repr(limit), "rows")
        e_chunk = expected.limit(limit)
        a_chunk = actual.limit(limit)
        row_diff = _diff_rows(expected=e_chunk,
                              actual=a_chunk)
        if row_diff:
            return row_diff
        if limit > max_rows:
            return _diff_ret(status='NO_ROW_DIFF_YET',
                             rows='NO_ROW_DIFF_YET')
        if limit > e_chunk.count():
            # No more data, return no diff
            # if e_chunk count
            return None
        limit = limit * multiplier


def print_diff_first_rows(a, b):
    """Print the diff of the values of the first rows of two data sets"""
    both = a.limit(1).union(b.limit(1))
    both_dict = both.toPandas().to_dict()
    for key in both_dict.keys():
        if both_dict[key][0] == both_dict[key][1]:
            continue
        print("DIFF", key, "- a:", both_dict[key][0], " b:", both_dict[key][1])


def _diff_ret(*,
              status,
              cols=None,
              dtypes=None,
              counts=None,
              rows="NOT_EVALUATED"):
    """Standardize diff return value structure"""
    return {'status': status,
            'cols': cols,
            'dtypes': dtypes,
            'counts': counts,
            'rows': rows
            }


def _diff_rows(*, expected, actual):
    rows_not_in_actual = expected.subtract(actual)
    rows_not_in_actual_count = rows_not_in_actual.count()
    print("rows_not_in_actual count " + str(rows_not_in_actual_count) + "")
    rows_not_in_expected = actual.subtract(expected)
    rows_not_in_expected_count = rows_not_in_expected.count()
    print("rows_not_in_expected count " + str(
        rows_not_in_expected_count) + "")
    all_diff_rows = rows_not_in_actual.union(rows_not_in_expected)
    all_diff_rows_count = all_diff_rows.count()
    print("Total different rows count " + str(all_diff_rows_count) + "")
    if all_diff_rows_count == 0:
        return None
    return _diff_ret(status='ROW_DIFF', rows={
        'rows_not_in_actual': rows_not_in_actual,
        'rows_not_in_expected': rows_not_in_expected
    })


def print_row_sets(expected, actual, max_rows=20):
    print("\n")
    lim_expected = expected.limit(max_rows)
    lim_actual = actual.limit(max_rows)
    # Are the records the same
    expected_pdf = panda_sorted(lim_expected)
    actual_pdf = panda_sorted(lim_actual)
    if not expected_pdf.equals(actual_pdf):
        expected_rows_str = expected_pdf.head(max_rows).to_string()
        actual_rows_str = actual_pdf.head(max_rows).to_string()
        print("""Differing rows of first %d differing rows:

Only in expected:
%s
Only in actual result:
%s
""" % (max_rows, expected_rows_str, actual_rows_str))
    print("""
First rows value diff:

""")
    print_diff_first_rows(expected, actual)


def panda_diff_rows(expected, actual, max_rows=20):
    print("\n")
    lim_expected = expected.limit(max_rows)
    lim_actual = actual.limit(max_rows)
    # Are the records the same
    expected_pdf = panda_sorted(lim_expected)
    actual_pdf = panda_sorted(lim_actual)
    if not expected_pdf.equals(actual_pdf):
        diff_df = lim_expected.subtract(lim_actual)
        reverse_diff_df = lim_actual.subtract(lim_expected)
        differing_rows_str = panda_sorted(diff_df).head(max_rows).to_string()
        reverse_differing_rows_str = panda_sorted(
            reverse_diff_df).head(max_rows).to_string()
        expected_rows_str = expected_pdf.head(max_rows).to_string()
        actual_rows_str = actual_pdf.head(max_rows).to_string()
        print("""Differing rows of %d first rows:

Only in expected:
%s
Only in actual result:
%s
Expected:
%s
Actual:
%s""" % (max_rows,
         differing_rows_str,
         reverse_differing_rows_str,
         expected_rows_str,
         actual_rows_str))


def panda_assert_df_equals(expected_df, actual_df):
    expected_pdf = panda_sorted(expected_df)
    actual_pdf = panda_sorted(actual_df)
    pd.testing.assert_frame_equal(expected_pdf,
                                  actual_pdf,
                                  check_like=True,
                                  check_dtype=True)


def panda_sorted(df):
    return df.toPandas().sort_values(df.columns).reset_index(drop=True)


# def assert_equals(expected, actual):
#     # Create richer diff information
#     diff_ret = diff(expected, actual)
#     if (diff_ret):
#         cols = actual.columns
#         col_strs = []
#         for idx, col in enumerate(cols):
#             col_strs.append("%d:%s" % (idx, col))
#         print("Columns:", ", ".join(col_strs))
#         print(diff_ret)
#     # Perform a deeper comparison to catch dtype and other diffs
#     panda_assert_df_equals(expected, actual)


def _array_diff(first, second):
    if len(first) != len(second):
        return True
    first = set(first)
    second = set(second)
    return [item for item in first if item not in second]


def _cols_diff(expected, actual):
    """Should have same columns"""

    if _array_diff(expected.columns, actual.columns):
        return _cols_diff_error(expected.columns, actual.columns)
    return None


def _cols_diff_error(expected, actual):
    print("Error: Cols diff\n\n")
    for i in range(len(expected)):
        if i >= len(actual):
            break
        if expected[i] != actual[i]:
            print(f"""First different column:
expected[{i}]: {expected[i]}
actual[{i}]:   {actual[i]}
""")
            break
    expected_cols = ",".join(expected)
    actual_cols = ",".join(actual)
    print("""
Expected: %s
Actual:   %s""" % (expected_cols, actual_cols))
    return {'expected_cols': expected_cols,
            'actual_cols': actual_cols}


def _dtypes_diff(expected, actual):
    """Should have same data types"""
    if _array_diff(expected.dtypes, actual.dtypes):
        print("actual.dtypes:", repr(actual.dtypes))
        expected_dtypes = repr(expected.dtypes)
        actual_dtypes = repr(actual.dtypes)
        print("""Error: Column type diff
Expected: %s
Actual:   %s""" % (expected_dtypes, actual_dtypes))
        return {'expected_dtypes': expected_dtypes,
                'actual_dtypes': actual_dtypes}
    return None
