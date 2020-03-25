import arrow
from arrow import Arrow
from pandas import DataFrame
from pkg.utils.df import create_df_with_zeroes, smooth_df, compare_df, shift_df, increase_df
from pkg.utils.series import is_series_correlated


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
