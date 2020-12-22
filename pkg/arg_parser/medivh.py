import argparse


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
        '-i',
        '--input',
        help='Path to input CSV file'
    )
    parser.add_argument(
        '-s',
        '--short',
        action='store_true',
        help='Output short result'
    )
    parser.add_argument(
        '-a',
        '--algorithm',
        choices=['default', 'mean'],
        default='default',
        help='Algorithm to get forecasts'
    )
    return parser.parse_args()
