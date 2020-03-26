import numpy as np
from pandas import Series


def get_series_correlation(first: Series, second: Series):
    return first.corr(second) * 100.0


def is_series_correlated(first: Series, second: Series):
    return get_series_correlation(first, second) >= 75.0


def get_series_accuracy(first: Series, second: Series):
    arr = []
    for index, sval in second.items():
        fval = first[index]
        diff = abs(fval - sval)
        perc = 100.0 - min(diff * 100 / fval, 100.0)
        arr.append(perc)
    return np.median(arr)
