#!/usr/bin/env python3

import argparse
import arrow
import csv
import yaml
import sys
from pkg.data import get_barcode_daily_sales, get_category_daily_sales
from pkg.forecast import get_barcode_forecast, get_category_forecast, get_mean_forecast
from pkg.utils.console import panic
from pkg.utils.files import read_file
from progress.bar import ChargingBar
from sqlalchemy import create_engine


def create_argparse():
    parser = argparse.ArgumentParser(description='Sales forecasts')
    parser.add_argument(
        '-c',
        '--config',
        required=True,
        help='Path to config file'
    )
    parser.add_argument(
        '-o',
        '--output',
        required=True,
        help='Path to output CSV file'
    )
    parser.add_argument(
        '-a',
        '--algorithm',
        choices=['default', 'mean'],
        default='default',
        help='Algorithm to get forecasts'
    )
    return parser.parse_args()


if __name__ == '__main__':
    if sys.version_info < (3, 8):
        panic('We need minimum Python version 3.8 to run. Current version: %s.%s.%s' % sys.version_info[:3])

    args = create_argparse()
    if args.config:
        if args.output:
            csv_file = open(args.output, 'w', newline='')
            csv_writer = csv.writer(csv_file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            config = yaml.safe_load(read_file(args.config))
            print(f'Config loaded from {args.config}')

            db = config['mysql']
            db_path = '%s:%s@%s:%s/%s' % (db['user'], db['pass'], db['host'], db['port'], db['schema'])
            db_secured_path = '%s:%s@%s:%s/%s' % (db['user'], '*****', db['host'], db['port'], db['schema'])
            engine = create_engine(f'mysql+mysqlconnector://{db_path}')
            print(f'Connected to MySQL at {db_secured_path}\n')

            print(f'Processing with {args.algorithm} algorithm...')
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
                    bar.message = f'[Store: {store_id}] {str(barcode).ljust(13, " ")}'
                    bar.update()
                    for p in range(len(periods)):
                        period = periods[p]
                        forecast_from_date = arrow.get(period['date'], 'DD.MM.YYYY')
                        forecast_before_date = forecast_from_date.shift(days=period['days'])

                        forecast = 0.0
                        df_barcode = get_barcode_daily_sales(engine, store_id, barcode)

                        if args.algorithm == 'default':
                            use_category_forecast = False
                            # noinspection PyBroadException
                            try:
                                barcode_forecast = get_barcode_forecast(df_barcode, forecast_from_date, forecast_before_date)
                                if barcode_forecast is not None:
                                    forecast = barcode_forecast.sum()
                                else:
                                    use_category_forecast = True
                            except:
                                use_category_forecast = True

                            if use_category_forecast:
                                df_category = get_category_daily_sales(engine, store_id, barcode)
                                # noinspection TryExceptPass, PyBroadException
                                try:
                                    category_forecast = get_category_forecast(df_barcode, df_category, forecast_from_date, forecast_before_date)
                                    if category_forecast is not None:
                                        forecast = category_forecast.sum()
                                except:
                                    pass
                        elif args.algorithm == 'mean':
                            # noinspection TryExceptPass, PyBroadException
                            try:
                                mean_forecast = get_mean_forecast(df_barcode, forecast_from_date, forecast_before_date)
                                if mean_forecast is not None:
                                    forecast = mean_forecast.sum()
                            except:
                                pass

                        csv_writer.writerow([store_id, barcode, period['date'], period['days'], round(forecast, 2)])
                        bar.next()

            bar.finish()
            csv_file.close()
            print(f'\nDone. Result was written to {args.output}')

        else:
            panic('No output file specified. Use -o option')

    else:
        panic('No config file specified. Use -c option')
