# Nyc Taxi data - experimental project

Do some experiments with the NYC taxi data

# How to run tests

## Install pipenv

``````bash
pipenv install --dev
``````

## Run tests from project folder:

``````bash
nyctaxi$ pytest tests/
=================================================================================== test session starts ====================================================================================
...

tests/libs/age/test_gen_age.py .                                                                                                                                                     [  7%]
tests/libs/chain/test_chain.py .                                                                                                                                                     [ 14%]
tests/libs/dataframe/test_dfdiff.py ...........                                                                                                                                      [ 92%]
tests/libs/periods/test_group_by_day.py .                                                                                                                                            [100%]

...
======================================================================== 14 passed, 14 warnings in 88.60s (0:01:28) ========================================================================
``````