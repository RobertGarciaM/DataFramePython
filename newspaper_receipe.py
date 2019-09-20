
import argparse
import logging
import hashlib
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
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_newlines_from_body(df)
    return df

def _generate_uids_for_rows(df):
    logger.info('Generating uid foreach row')    

    uids = (df.apply(lambda row: hashlib.md5(bytes(row['url'].encode())) , axis=1)
              .apply(lambda hash_object: hash_object.hexdigest())
           )
    
    df['uid'] = uids
    return df.set_index('uid')

def _remove_newlines_from_body(df):
    logger.info('removing new lines from body')

    stripped_body = (df
                        .astype(str)
                        .apply(lambda row: row['body'], axis = 1)
                        .apply(lambda body: list(body))
                        .apply(lambda letters: 
                                list(map(lambda letter: letter.replace('\n',''), letters)))
                        .apply(lambda letters: ''.join(letters))
        
                    )
    df['body'] = stripped_body
    return df
    

#  Esta funciìn toma el df y remplaza los NAN o NUlL del title
#  por un title que se encuentra en la url
def _fill_missing_titles(df):
    logger.info('Filling missing tittles')
    missing_titles_mask = df['title'].isna()
    #print(missing_titles_mask)

    missing_titles = (df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .astype(str).applymap(lambda title: title.split('_'))
                        .applymap(lambda title_word_list: ''.join(title_word_list))    
                     )

    #print(missing_titles.loc[:, 'missing_titles'])
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
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