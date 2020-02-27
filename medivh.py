import arrow
import numpy as np
import pandas as pd
from arrow import Arrow
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
from sqlalchemy import create_engine
from typing import List, Callable, Union

engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')


def create_df_indexed_by_date(data_frame: DataFrame, index_col: str = 'date_idx') -> DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(data_frame[index_col]))
    result = data_frame.set_index(index).sort_index()
    result.drop(index_col, axis=1, inplace=True)
    return result


def create_df_with_zeroes(data_frame: DataFrame,
                          beg: Arrow,
                          end: Arrow,
                          date_selector: Union[Callable, None] = None
                          ) -> DataFrame:
    arr = []
    for day in Arrow.range('day', beg, end):
        dt = date_selector(day) if date_selector else day
        try:
            value = data_frame.loc[dt.date()].values[0]
        except KeyError:
            value = 0.0
        arr.append([day.date(), value])
    return create_df_indexed_by_date(DataFrame(arr, columns=['date_idx', 'quantity']))


def create_array_with_zeroes(data_frame: DataFrame, beg: Arrow, end: Arrow) -> List[float]:
    arr = []
    for day in Arrow.range('day', beg, end):
        try:
            value = data_frame.loc[day.date()].values[0]
        except KeyError:
            value = 0.0
        arr.append(value)
    return arr


def combine_series(s1: Series, s2: Series):
    def add_percent(v, percent):
        return v * (1 + (percent / 100))

    def is_nan_or_zero(v):
        return np.isnan(v) or v == 0.0

    def mean_with_weight(v1, v2):
        if is_nan_or_zero(v1):
            return add_percent(v2, -10)
        else:
            return add_percent(np.mean([v1, v2]), 10)

    return s1.combine(s2, mean_with_weight)


def get_daily_sales_by_barcode(barcode: int) -> DataFrame:
    data = pd.read_sql(f'select date as date_idx, '
                       f'       quantity '
                       f'from   medivh.sales__by_day '
                       f'where  barcode = {barcode}', con=engine)
    return create_df_indexed_by_date(data)


def get_forecast(data_frame: DataFrame, for_date: Arrow, strategy: str = 'mean_past_week') -> float:
    switcher = {
        'mean_past_week': (for_date.shift(days=-7), for_date.shift(days=-1)),
        'mean_past_year_week': (for_date.shift(days=-7).shift(years=-1), for_date.shift(days=-1).shift(years=-1)),
    }
    beg, end = switcher.get(strategy)
    arr = create_array_with_zeroes(data_frame, beg, end)
    return float(np.mean(arr))


def get_forecast_by_period(data_frame: DataFrame, beg: Arrow, end: Arrow, strategy: str = 'mean_past_week'):
    data = [[day.date(), get_forecast(data_frame, day, strategy)] for day in Arrow.range('day', beg, end)]
    return create_df_indexed_by_date(DataFrame(data, columns=['date_idx', 'quantity']))


beg_date = arrow.get(2020, 1, 1)
end_date = arrow.get(2020, 4, 30)
# 8887290101004 - coffee
# 5449000133328 - coca
# 48742245      - parliament
# 48743587      - winston
df = get_daily_sales_by_barcode(48743587)

forecast_1 = get_forecast_by_period(df, beg_date, end_date, 'mean_past_week')
forecast_2 = get_forecast_by_period(df, beg_date, end_date, 'mean_past_year_week')
real = create_df_with_zeroes(df, beg_date, end_date)
# old = create_df_with_zeroes(df, beg_date, end_date, lambda a: a.shift(years=-1))

# real.insert(len(real.columns), 'old', old)
real.insert(len(real.columns), 'forecast_1', forecast_1)
real.insert(len(real.columns), 'forecast_2', forecast_2)
real.insert(len(real.columns), 'combined', forecast_1.combine(forecast_2, combine_series))

real.plot()
plt.show()
