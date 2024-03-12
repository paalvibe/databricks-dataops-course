import datetime

from pyspark.sql import functions as F


def gen_age(*,
              df,
              today: datetime.date,
              datecol: str):
    """
    Generate an age variable 'age' based on todays date

    :param df: spark df
    :param today: datetime.date
    :param datecol: str
    :return: en spark df
    """

    df = df.withColumn('today', F.lit(today))
    # Not 100% correct, but good enough for now
    df = df.withColumn(
        'age', F.floor(F.datediff(F.col('today'), F.col(datecol)) / 365))

    # remove temp col
    df = df.drop('today')

    return df
