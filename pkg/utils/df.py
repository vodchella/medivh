import numpy as np
import pandas as pd
from arrow import Arrow
from pandas import DataFrame
from typing import List, Callable, Union

IDX_COL = 'date_idx'


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


def compare_df(new_df: DataFrame, old_df: DataFrame, beg: Arrow, end: Arrow):
    new_mean = new_df.loc[beg.date():end.date()].mean().values[0]
    old_mean = old_df.loc[beg.date():end.date()].mean().values[0]
    if old_mean != 0.0:
        percent_diff = (new_mean * 100 / old_mean) - 100
        diff = new_mean - old_mean
    else:
        raise Exception('No data for past year')
    return percent_diff, diff


def modify_df(beg: Arrow, end: Arrow, modifier: Callable):
    arr = []
    for day in Arrow.range('day', beg, end):
        arr.append([day.date(), modifier(day)])
    return create_df_indexed_by_date(DataFrame(arr, columns=[IDX_COL, 'quantity']))


def smooth_df(data_frame: DataFrame, beg: Arrow, end: Arrow):
    def fn(day):
        # Simple moving average algorithm
        past_week = create_array_with_zeroes(data_frame, day.shift(days=-6), day)
        return float(np.mean(past_week))
    return modify_df(beg, end, fn)


def increase_df(data_frame: DataFrame, inc_percent: float, beg: Arrow, end: Arrow):
    def fn(day):
        val = data_frame.loc[day.date()].values[0]
        return val + (val * inc_percent / 100)
    return modify_df(beg, end, fn)


def shift_df(data_frame: DataFrame, shift: float, beg: Arrow, end: Arrow):
    def fn(day):
        val = data_frame.loc[day.date()].values[0]
        return val + shift
    return modify_df(beg, end, fn)


def merge_df(first: DataFrame, second: DataFrame):
    combined = pd.concat([first, second])
    by_row_index = combined.groupby(combined.index)
    return by_row_index.mean()
