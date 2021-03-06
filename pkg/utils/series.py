import numpy as np
from pandas import Series


def percent_accuracy_errors(fval: float, sval: float) -> float:
    if fval != 0.0:
        diff = abs(fval - sval)
        perc = diff * 100 / fval
    else:
        perc = sval
    return perc


def get_series_correlation(first: Series, second: Series):
    return first.corr(second) * 100.0


def is_series_correlated(first: Series, second: Series):
    return get_series_correlation(first, second) >= 75.0


def get_forecast_accuracy_errors(real: Series, forecast: Series):
    arr = []
    for index, value in forecast.items():
        arr.append(percent_accuracy_errors(real[index], value))
    return round(float(np.median(arr)), 2)


def get_forecast_standard_deviation(real: Series, forecast: Series):
    arr = []
    for index, value in forecast.items():
        arr.append(real[index] - value)
    return round(float(np.std(arr)), 2)
