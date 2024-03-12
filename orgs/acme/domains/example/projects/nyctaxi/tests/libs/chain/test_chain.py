import pytest  # noqa F401
from libs.chain import chain


def test_chain():  # noqa F811
    start_num = 50
    normal_calc = divide10(plus100(start_num))
    print("normal_calc", normal_calc)
    chained_calc = chain(start_num,
                         [
                             plus100,
                             divide10
                         ])
    print("chained_calc", chained_calc)
    assert normal_calc == chained_calc


def plus100(num):
    return num + 100


def divide10(num):
    return num / 10
