from pandas import Series


def get_series_correlation(first: Series, second: Series):
    return first.corr(second)


def is_series_correlated(first: Series, second: Series):
    return get_series_correlation(first, second) >= 0.75
