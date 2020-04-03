#!/usr/bin/env python3

import argparse
import arrow
import csv
import yaml
import sys
from pkg.data import get_barcode_daily_sales, create_engine
from pkg.utils.console import panic
from pkg.utils.files import read_file
from progress.bar import ChargingBar


def create_argparse():
    parser = argparse.ArgumentParser(description='Forecasts tester')
    parser.add_argument(
        '-c',
        '--config',
        help='Path to config file'
    )
    parser.add_argument(
        '-o',
        '--output',
        help='Path to output CSV file'
    )
    parser.add_argument(
        '-g',
        '--generate',
        action='store_true',
        help='Generate real sales data'
    )
    return parser.parse_args()


if __name__ == '__main__':
    if sys.version_info < (3, 8):
        panic('We need minimum Python version 3.8 to run. Current version: %s.%s.%s' % sys.version_info[:3])

    args = create_argparse()
    if args.generate:
        if args.config:
            if args.output:
                csv_file = open(args.output, 'w', newline='')
                csv_writer = csv.writer(csv_file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)

                config = yaml.safe_load(read_file(args.config))
                print(f'Config loaded from {args.config}')

                engine = create_engine(config['mysql'])

                print(f'Processing...')
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

                        df_barcode = get_barcode_daily_sales(engine, store_id, barcode)

                        for p in range(len(periods)):
                            period = periods[p]
                            today = arrow.get(period['date'], 'DD.MM.YYYY')
                            beg = today.shift(days=1)
                            end = beg.shift(days=period['days'])

                            sales = df_barcode.loc[beg.date():end.date()]
                            sales_sum = sales['quantity'].sum()

                            csv_writer.writerow([store_id, barcode, period['date'], period['days'], round(sales_sum, 2)])
                            bar.next()

                bar.finish()
                csv_file.close()
                print(f'\nDone. Result was written to {args.output}')

            else:
                panic('No output file specified. Use -o option')

        else:
            panic('No config file specified. Use -c option')

    else:
        panic('Nothing to do. Call tester with -h option')
