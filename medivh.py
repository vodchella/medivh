import arrow
import numpy as np
import pandas as pd
from arrow import Arrow
import matplotlib.pyplot as plt
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine
from typing import List

engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')


def create_df_indexed_by_date(data_frame: DataFrame, index_col: str = 'date_idx') -> DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(data_frame[index_col]))
    result = data_frame.set_index(index).sort_index()
    result.drop(index_col, axis=1, inplace=True)
    return result


def get_daily_sales_by_barcode(barcode: int) -> DataFrame:
    data = pd.read_sql(f'select date as date_idx, '
                       f'       quantity '
                       f'from   medivh.sales__by_day '
                       f'where  barcode = {barcode}', con=engine)
    return create_df_indexed_by_date(data)


def get_array_with_zeroes(data_frame: DataFrame, beg: Arrow, end: Arrow) -> List[float]:
    arr = []
    for day in Arrow.range('day', beg, end):
        try:
            value = data_frame.loc[day.date()].values[0]
        except KeyError:
            value = 0.0
        arr.append(value)
    return arr


def get_data_frame_with_zeroes(data_frame: DataFrame, beg: Arrow, end: Arrow) -> DataFrame:
    arr = []
    for day in Arrow.range('day', beg, end):
        try:
            value = data_frame.loc[day.date()].values[0]
        except KeyError:
            value = 0.0
        arr.append([day.date(), value])
    return create_df_indexed_by_date(DataFrame(arr, columns=['date_idx', 'quantity']))


def get_forecast(data_frame: DataFrame, for_date: Arrow) -> float:
    beg = for_date.shift(days=-7)
    end = for_date.shift(days=-1)
    arr = get_array_with_zeroes(data_frame, beg, end)
    return float(np.median(arr))


def get_forecast_by_period(data_frame: DataFrame, beg: Arrow, end: Arrow):
    data = [[day.date(), get_forecast(data_frame, day)] for day in Arrow.range('day', beg, end)]
    return create_df_indexed_by_date(DataFrame(data, columns=['date_idx', 'quantity']))


beg_date = arrow.get(2019, 1, 1)
end_date = arrow.get(2019, 5, 30)
df = get_daily_sales_by_barcode(5449000133328)
forecast = get_forecast_by_period(df, beg_date, end_date)
real = get_data_frame_with_zeroes(df, beg_date, end_date)
real.insert(1, 'forecast', forecast.loc[:])

real.plot()
plt.show()
