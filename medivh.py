#!/usr/bin/env python3

import arrow
import csv
import yaml
import sys
from pkg.arg_parser.medivh import create_argparse
from pkg.data import get_barcode_daily_sales, create_engine
from pkg.forecast import get_barcode_forecast, get_mean_forecast
from pkg.utils.console import panic, write_stdout, write_stderr
from pkg.utils.files import read_file
from progress.bar import ChargingBar
from sh import Command

wc: Command
awk: Command
try:
    from sh import wc, sed
except ImportError as e:
    write_stderr(f'{str(e)}\n')


CONFIG = None


def do_forecast(algorithm, barcode_dataframe, forecast_from_date, forecast_before_date):
    forecast = 0.0

    if algorithm == 'default':
        use_mean_forecast = False
        # noinspection PyBroadException
        try:
            barcode_forecast = get_barcode_forecast(barcode_dataframe, forecast_from_date, forecast_before_date)
            if barcode_forecast is not None:
                forecast = barcode_forecast.sum()
            else:
                use_mean_forecast = True
        except:
            use_mean_forecast = True

        if use_mean_forecast:
            # noinspection TryExceptPass, PyBroadException
            try:
                mean_forecast = get_mean_forecast(barcode_dataframe, forecast_from_date, forecast_before_date)
                if mean_forecast is not None:
                    forecast = mean_forecast.sum()
            except:
                pass

    elif algorithm == 'mean':
        # noinspection TryExceptPass, PyBroadException
        try:
            mean_forecast = get_mean_forecast(barcode_dataframe, forecast_from_date, forecast_before_date)
            if mean_forecast is not None:
                forecast = mean_forecast.sum()
        except:
            pass

    return round(forecast, 2)


def process_default(out_file, algorithm):
    global CONFIG

    csv_file = open(out_file, 'w', newline='')
    csv_writer = csv.writer(csv_file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    engine = create_engine(CONFIG['mysql'])

    print(f'Processing with {algorithm} algorithm...')
    stores = CONFIG['stores']
    barcodes = CONFIG['barcodes']
    periods = CONFIG['periods']
    iter_cnt = len(stores) * len(barcodes) * len(periods)
    bar = ChargingBar('Waiting...', max=iter_cnt)
    bar.start()

    for store_id in stores:
        for barcode in barcodes:
            bar.message = f'[Store: {store_id}] {str(barcode).ljust(13, " ")}'
            bar.update()

            df_barcode = get_barcode_daily_sales(engine, store_id, barcode)

            for period in periods:
                forecast_from_date = arrow.get(period['date'], 'DD.MM.YYYY')
                forecast_before_date = forecast_from_date.shift(days=period['days'])

                forecast = do_forecast(algorithm, df_barcode, forecast_from_date, forecast_before_date)

                csv_writer.writerow([store_id, barcode, period['date'], period['days'], forecast])
                bar.next()

    bar.finish()
    csv_file.close()
    print(f'\nDone. Result was written to {args.output}')


def process_short(out_file, in_file, algorithm):
    global CONFIG

    beg_date = arrow.get('2019-10-01', 'YYYY-MM-DD')
    end_date = arrow.get('2020-01-31', 'YYYY-MM-DD')
    write_stdout(f'Forecast from {beg_date} to {end_date}\n')  # 123 days

    lines_count = 0
    if wc and sed:
        write_stdout(f'Counting {in_file} non-blank lines...  ')
        lines_count = int(wc(sed(r'/^\s*$/d', in_file), '-l'))
        print(lines_count)
    ops_count = lines_count * 123

    out_csv_file = open(out_file, 'w', newline='')
    in_csv_file = open(in_file, 'r', newline='\n')
    csv_writer = csv.writer(out_csv_file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csv_reader = csv.reader(in_csv_file, delimiter=',')

    engine = create_engine(CONFIG['mysql'])
    print(f'Processing short output with {algorithm} algorithm...')

    bar = None
    if ops_count > 0:
        bar = ChargingBar('Waiting...', max=ops_count)
        bar.start()

    i = 0
    dfs = {}
    for row in csv_reader:
        if row is not None and len(row):
            store_id = int(row[0])
            barcode = int(row[1])

            key = f'{store_id}-{barcode}'
            try:
                df_barcode = dfs[key]
            except KeyError:
                df_barcode = get_barcode_daily_sales(engine, store_id, barcode)
                dfs[key] = df_barcode

            for j, d in enumerate(arrow.Arrow.range('day', beg_date, end_date)):
                forecast_from_date = d
                forecast_before_date = forecast_from_date.shift(days=5)

                forecast = do_forecast(algorithm, df_barcode, forecast_from_date, forecast_before_date)
                csv_writer.writerow([int(round(forecast))])

                if bar:
                    curr_op = i * 123 + j
                    if curr_op % 5 == 0:
                        bar.message = f'{curr_op} of {ops_count}'
                        bar.update()
                    bar.next()
            i += 1

    bar.message = 'Done'
    bar.update()

    out_csv_file.close()
    in_csv_file.close()


if __name__ == '__main__':
    if sys.version_info < (3, 8):
        panic('We need minimum Python version 3.8 to run. Current version: %s.%s.%s' % sys.version_info[:3])

    args = create_argparse()
    if args.config:
        CONFIG = yaml.safe_load(read_file(args.config))
        print(f'Config loaded from {args.config}')

        if args.output:
            if args.short:
                if args.input:
                    process_short(args.output, args.input, args.algorithm)
                else:
                    panic('No input file specified. Use -i option')
            else:
                process_default(args.output, args.algorithm)
        else:
            panic('No output file specified. Use -o option')
    else:
        panic('No config file specified. Use -c option')
