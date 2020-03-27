import numpy as np
from pandas import Series


def percent_accuracy(fval: float, sval: float) -> float:
    if fval != 0.0:
        diff = abs(fval - sval)
        perc = 100.0 - min(diff * 100 / fval, 100.0)
    else:
        perc = 0.0
    return perc


def get_series_correlation(first: Series, second: Series):
    return first.corr(second) * 100.0


def is_series_correlated(first: Series, second: Series):
    return get_series_correlation(first, second) >= 75.0


def get_series_accuracy(first: Series, second: Series):
    arr = []
    for index, value in second.items():
        arr.append(percent_accuracy(first[index], value))
    return np.median(arr)


def get_series_value_accuracy(first: Series, second: Series):
    return percent_accuracy(first.sum(), second.sum())
