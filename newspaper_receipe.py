
import argparse
import logging
logging.basicConfig(level = logging.INFO)
from urllib.parse import urlparse

import pandas as pd

logger = logging.getLogger(__name__)


def main(filename):
    logger.info('Starting claning proccess')

    df = _read_data(filename)
    newspaper_uid = _extract_newspapaer_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    return df

def _extract_host(df):
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uid column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df

def _extract_newspapaer_uid(filename):
    logger.info('Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]

    logger.info('NewsPaper uid detected: {}'.format(newspaper_uid))
    return newspaper_uid

def _read_data(filename):
    logging.info('Readin file {}'.format(filename))

    return pd.read_csv(filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help = 'The path the dirty data',
                        type = str)

    arg = parser.parse_args()

    df = main(arg.filename)
    print(df)