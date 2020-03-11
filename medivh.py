import arrow
import numpy as np
import pandas as pd
from arrow import Arrow
import matplotlib.pyplot as plt
from pandas import DataFrame, Series
from sqlalchemy import create_engine
from typing import List, Callable, Union

IDX_COL = 'date_idx'


def get_barcode_daily_sales(store_id: int, code: int) -> DataFrame:
    engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')
    data = pd.read_sql(f'select date as date_idx, '
                       f'       quantity '
                       f'from   medivh.sales__by_barcode_by_day '
                       f'where  barcode = {code} and store_id = {store_id}', con=engine)
    return create_df_indexed_by_date(data)


def get_category_daily_sales(store_id: int, code: int) -> DataFrame:
    engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')
    data = pd.read_sql(f'select sc.date as date_idx, '
                       f'       sc.quantity '
                       f'from   medivh.sales__by_category_by_day sc '
                       f'where  sc.store_id = {store_id} and '
                       f'       sc.category_id = (select p.category_id '
                       f'                         from   medivh.product p '
                       f'                         where  p.barcode = {code} and '
                       f'                                p.store_group_id = (select s.store_group_id '
                       f'                                                    from   medivh.store s '
                       f'                                                    where  s.id = sc.store_id)) ', con=engine)
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


def is_series_correlated(first: Series, second: Series):
    return first.corr(second) >= 0.75


def get_barcode_forecast(data_frame: DataFrame, now: Arrow, for_date: Arrow) -> DataFrame:
    if not data_frame.empty:
        last_data_date = arrow.get(data_frame.index.max())

        beg_date = now.shift(months=-1)
        end_date = for_date

        real = create_df_with_zeroes(data_frame, beg_date.shift(weeks=-1), end_date)
        old = create_df_with_zeroes(data_frame, beg_date.shift(weeks=-1), end_date, lambda a: a.shift(years=-1))
        real_smoothed = smooth_df(real, beg_date, last_data_date)
        old_smoothed = smooth_df(old, beg_date, end_date)

        if not real_smoothed.empty:
            _, diff = compare_df(real_smoothed, old_smoothed, now.shift(days=1), end_date)
            result_forecast = shift_df(old_smoothed, diff, now.shift(days=1), end_date)

            result = real_smoothed
            result.columns = ['this_year_sales']
            result.insert(len(result.columns), 'past_year_sales', old_smoothed)
            result.insert(len(result.columns), 'forecast', result_forecast)
            return result
        else:
            return real_smoothed  # Return empty data frame
    else:
        raise Exception('Empty data frame')


def get_category_forecast(barcode_data_frame: DataFrame, category_data_frame: DataFrame, now: Arrow, for_date: Arrow):
    tomorrow = now.shift(days=1)
    past_month = now.shift(months=-1)

    barcode_df = create_df_with_zeroes(barcode_data_frame, past_month, now)
    barcode_smoothed = smooth_df(barcode_df, past_month, now)
    category_df = create_df_with_zeroes(category_data_frame, past_month, for_date, lambda a: a.shift(years=-1))
    category_smoothed = smooth_df(category_df, past_month, for_date)

    barcode_series = barcode_smoothed['quantity'][past_month.date():now.date()]
    category_series = category_smoothed['quantity'][past_month.date():now.date()]
    if is_series_correlated(barcode_series, category_series):
        percent, _ = compare_df(barcode_series.to_frame(), category_series.to_frame(), past_month, now)
        forecast = category_smoothed['quantity'][tomorrow.date():for_date.date()]
        forecast_normalized = increase_df(forecast.to_frame(), percent, tomorrow, for_date)

        result = category_smoothed
        result.columns = ['past_year_category_sales']
        result.insert(len(result.columns), 'past_month_product_sales', barcode_smoothed)
        result.insert(len(result.columns), 'forecast', forecast_normalized)

        return result
    else:
        print('No correlation with past year category sales, forecast impossible')


products = {
    8887290101004: 'Coffee 3 in 1',
    5449000133328: 'Coca cola',
    4870204391510: 'Dizzy',
    4680036912629: 'Gorilla',
    48742245: 'Parliament',
    48743587: 'Winston',
}


barcode = 48743587
store_id = 110
today = arrow.get(2020, 1, 25)
forecast_before_date = today.shift(months=1)

df_barcode = get_barcode_daily_sales(store_id, barcode)
df_category = get_category_daily_sales(store_id, barcode)

barcode_forecast = get_barcode_forecast(df_barcode, today, forecast_before_date)
if barcode_forecast is not None:
    try:
        barcode_forecast.plot(title=products[barcode])
        plt.show()
    except TypeError:
        print('[1] no data to plot')

category_forecast = get_category_forecast(df_barcode, df_category, today, forecast_before_date)
if category_forecast is not None:
    try:
        category_forecast.plot(title=products[barcode])
        plt.show()
    except TypeError:
        print('[2] No data to plot')
