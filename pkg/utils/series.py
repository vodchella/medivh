from pandas import Series


def is_series_correlated(first: Series, second: Series):
    return first.corr(second) >= 0.75
