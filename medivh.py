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


def get_daily_sales_by_barcode(barcode: int) -> DataFrame:
    data = pd.read_sql(f'select date as date_idx, '
                       f'       quantity '
                       f'from   medivh.sales__by_day '
                       f'where  barcode = {barcode}', con=engine)
    return create_df_indexed_by_date(data)


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


def compare_df(new_df: DataFrame, old_df: DataFrame, beg: Arrow, end: Arrow) -> float:
    arr = []
    for day in Arrow.range('day', beg, end):
        new_val = new_df.loc[day.date()].values[0]
        old_val = old_df.loc[day.date()].values[0]
        percent_diff = 100 - (old_val * 100 / new_val)
        arr.append(percent_diff)
    return float(np.mean(arr))


def increase_df(data_frame: DataFrame, inc_percent: float, beg: Arrow, end: Arrow):
    arr = []
    for day in Arrow.range('day', beg, end):
        val = data_frame.loc[day.date()].values[0]
        new_val = val + (val * inc_percent / 100)
        arr.append([day.date(), new_val])
    return create_df_indexed_by_date(DataFrame(arr, columns=[IDX_COL, 'quantity']))


# 8887290101004 - coffee
# 5449000133328 - coca
# 48742245      - parliament
# 48743587      - winston

last_data_date = arrow.get(2020, 2, 26)
for_date = last_data_date.shift(months=1)

beg_date = last_data_date.shift(months=-1)
end_date = for_date
df = get_daily_sales_by_barcode(48743587)

real = create_df_with_zeroes(df, beg_date.shift(weeks=-1), end_date)
old = create_df_with_zeroes(df, beg_date.shift(weeks=-1), end_date, lambda a: a.shift(years=-1))
real_smoothed = smooth_df(real, beg_date, last_data_date)
old_smoothed = smooth_df(old, beg_date, end_date)

percent = compare_df(real_smoothed, old_smoothed, beg_date, last_data_date)
forecast = increase_df(old_smoothed, percent, last_data_date.shift(days=1), end_date)

real.insert(len(real.columns), 'smoothed', real_smoothed)
real.insert(len(real.columns), 'old', old_smoothed)
real.insert(len(real.columns), 'forecast', forecast)

real.columns = ['this_year_sales', 'this_year_sales_smoothed', 'past_year_sales_smoothed', 'forecast']

real.plot()
plt.show()
