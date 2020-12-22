import argparse


def create_argparse():
    parser = argparse.ArgumentParser(description='Forecasts tester')
    parser.add_argument(
        '-g',
        '--generate',
        action='store_true',
        help='Generate real sales data'
    )
    parser.add_argument(
        '-b',
        '--benchmark',
        action='store_true',
        help='Process benchmark'
    )
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
        '-s',
        '--sales',
        help='Path to real sales CSV file'
    )
    parser.add_argument(
        '-f',
        '--forecasts',
        nargs='+',
        help='List of files with forecasts'
    )
    parser.add_argument(
        '-i',
        '--image',
        help='Path to image file with plot result'
    )
    return parser.parse_args()
