#!/usr/bin/env python3

import argparse
from time import sleep

import arrow
import matplotlib.pyplot as plt
import yaml
from pandas import Series
from pkg.data import get_barcode_daily_sales, get_category_daily_sales
from pkg.forecast import get_barcode_forecast, get_category_forecast, get_mean_forecast
from pkg.utils.console import panic
from pkg.utils.df import create_df_with_zeroes
from pkg.utils.files import read_file
from pkg.utils.series import get_forecast_accuracy_errors, get_forecast_standard_deviation
from progress.bar import ChargingBar
from sqlalchemy import create_engine


def analyze_forecast(sales: Series, forecast: Series):
    accuracy_errors = get_forecast_accuracy_errors(sales, forecast)
    standard_deviation = get_forecast_standard_deviation(sales, forecast)
    print(f'... accuracy_errors: {accuracy_errors}%')
    print(f'... standard_deviation: {standard_deviation}')


def create_argparse():
    parser = argparse.ArgumentParser(description='Sales forecast')
    parser.add_argument(
        '-c',
        '--config',
        help='Path to config file'
    )
    return parser.parse_args()


# products = {
#     8887290101004: 'Coffee 3 in 1',
#     5449000133328: 'Coca cola',
#     4870204391510: 'Dizzy',
#     4680036912629: 'Gorilla',
#     48742245: 'Parliament',
#     48743587: 'Winston',
# }
#
#
# barcode = 48743587
# store_id = 450
# forecast_from_date = arrow.get(2019, 5, 25)
# forecast_before_date = forecast_from_date.shift(days=5)

args = create_argparse()
if args.config:
    config = yaml.safe_load(read_file(args.config))
    print(f'Config loaded from {args.config}')

    db = config['mysql']
    db_path = '%s:%s@%s:%s/%s' % (db['user'], db['pass'], db['host'], db['port'], db['schema'])
    db_secured_path = '%s:%s@%s:%s/%s' % (db['user'], '*****', db['host'], db['port'], db['schema'])
    engine = create_engine(f'mysql+mysqlconnector://{db_path}')
    print(f'Connected to MySQL at {db_secured_path}\n')

    stores = config['stores']
    barcodes = config['barcodes']
    periods = config['periods']
    iter_cnt = len(stores) * len(barcodes) * len(periods)
    bar = ChargingBar('Waiting...', max=iter_cnt)
    bar.start()
    for s in range(len(stores)):
        store_id = stores[s]
        for b in range(len(barcodes)):
            barcode = barcodes[b]
            bar.message = f'[Store: {store_id}] {barcode}'
            bar.update()
            for p in range(len(periods)):
                sleep(1)
                bar.next()
    bar.finish()

    # df_barcode = get_barcode_daily_sales(engine, store_id, barcode)
    # df_category = get_category_daily_sales(engine, store_id, barcode)
    #
    # real_sales = create_df_with_zeroes(df_barcode, forecast_from_date, forecast_before_date)
    # sales_series = real_sales['quantity'][forecast_from_date.date():forecast_before_date.date()]
    #
    # try:
    #     barcode_forecast = get_barcode_forecast(df_barcode, forecast_from_date, forecast_before_date)
    #     if barcode_forecast is not None:
    #         print('First forecast:')
    #         analyze_forecast(sales_series, barcode_forecast)
    #         real_sales.insert(len(real_sales.columns), 'forecast_1', barcode_forecast)
    # except:
    #     print('Can\'t get first forecast')
    #
    # try:
    #     category_forecast = get_category_forecast(df_barcode, df_category, forecast_from_date, forecast_before_date)
    #     if category_forecast is not None:
    #         print('Second forecast:')
    #         analyze_forecast(sales_series, category_forecast)
    #         real_sales.insert(len(real_sales.columns), 'forecast_2', category_forecast)
    # except:
    #     print('Can\'t get second forecast')
    #
    # try:
    #     mean_forecast = get_mean_forecast(df_barcode, forecast_from_date, forecast_before_date)
    #     if mean_forecast is not None:
    #         print('Third forecast:')
    #         analyze_forecast(sales_series, mean_forecast)
    #         real_sales.insert(len(real_sales.columns), 'forecast_3', mean_forecast)
    # except:
    #     print('Can\'t get third forecast')
    #
    # real_sales.plot(title=products[barcode])
    # plt.show()

else:
    panic('No config file specified. Use -c option')
