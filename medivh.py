import arrow
import matplotlib.pyplot as plt
from pkg.data import get_barcode_daily_sales, get_category_daily_sales
from pkg.forecast import get_barcode_forecast, get_category_forecast
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
today = arrow.get(2020, 1, 25)
forecast_before_date = today.shift(months=1)

engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')
df_barcode = get_barcode_daily_sales(engine, store_id, barcode)
df_category = get_category_daily_sales(engine, store_id, barcode)

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
