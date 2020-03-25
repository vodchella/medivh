import arrow
import matplotlib.pyplot as plt
from pkg.data import get_barcode_daily_sales, get_category_daily_sales
from pkg.forecast import get_barcode_forecast, get_category_forecast
from pkg.utils.df import create_df_with_zeroes, smooth_df
from pkg.utils.series import get_series_correlation
from sqlalchemy import create_engine


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
forecast_from_date = arrow.get(2020, 1, 25)
forecast_before_date = forecast_from_date.shift(months=1)

engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')
df_barcode = get_barcode_daily_sales(engine, store_id, barcode)
df_category = get_category_daily_sales(engine, store_id, barcode)

real_sales = smooth_df(create_df_with_zeroes(df_barcode, forecast_from_date, forecast_before_date),
                       forecast_from_date,
                       forecast_before_date)
sales_series = real_sales['quantity'][forecast_from_date.date():forecast_before_date.date()]

barcode_forecast = get_barcode_forecast(df_barcode, forecast_from_date, forecast_before_date)
if barcode_forecast is not None:
    print('First forecast:')
    print('... correlation: ', get_series_correlation(sales_series, barcode_forecast))
    real_sales.insert(len(real_sales.columns), 'forecast_1', barcode_forecast)

category_forecast = get_category_forecast(df_barcode, df_category, forecast_from_date, forecast_before_date)
if category_forecast is not None:
    print('Second forecast:')
    print('... correlation: ', get_series_correlation(sales_series, category_forecast))
    real_sales.insert(len(real_sales.columns), 'forecast_2', category_forecast)

real_sales.plot(title=products[barcode])
plt.show()
