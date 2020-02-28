import arrow
import numpy as np
import pandas as pd
from arrow import Arrow
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
from sqlalchemy import create_engine
from typing import List, Callable, Union

IDX_COL = 'date_idx'
engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')


def create_df_indexed_by_date(data_frame: DataFrame, index_col: str = IDX_COL) -> DataFrame:
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
    return create_df_indexed_by_date(DataFrame(arr, columns=[IDX_COL, 'quantity']))


def create_array_with_zeroes(data_frame: DataFrame, beg: Arrow, end: Arrow) -> List[float]:
    arr = []
    for day in Arrow.range('day', beg, end):
        try:
            value = data_frame.loc[day.date()].values[0]
        except KeyError:
            value = 0.0
        arr.append(value)
    return arr


def smooth_df(data_frame: DataFrame, beg: Arrow, end: Arrow):
    # Simple moving average algorithm
    arr = []
    for day in Arrow.range('day', beg, end):
        past_week = create_array_with_zeroes(data_frame, day.shift(days=-6), day)
        arr.append([day.date(), float(np.mean(past_week))])
    return create_df_indexed_by_date(DataFrame(arr, columns=[IDX_COL, 'quantity']))


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


# 8887290101004 - coffee
# 5449000133328 - coca
# 48742245      - parliament
# 48743587      - winston

now = arrow.get(2020, 2, 26)
beg_date = arrow.get(2019, 12, 1)
end_date = arrow.get(2020, 4, 30)
df = get_daily_sales_by_barcode(48743587)

real = create_df_with_zeroes(df, beg_date.shift(weeks=-1), end_date)
old = create_df_with_zeroes(df, beg_date.shift(weeks=-1), end_date, lambda a: a.shift(years=-1))
real_smoothed = smooth_df(real, beg_date, now)
old_smoothed = smooth_df(old, beg_date, end_date)

real.columns = ['real']
real.insert(len(real.columns), 'smoothed', real_smoothed)
real.insert(len(real.columns), 'old', old_smoothed)

real.plot()
plt.show()
