import argparse
import hashlib
import nltk
from nltk.corpus import stopwords
import logging
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse

import pandas as pd

stop_words = set(stopwords.words('spanish'))
logger = logging.getLogger(__name__)

def main(filename):
    logger.info('Starting cleanin process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_from_body(df)
    df = _tokenize_column(df, 'title')
    df = _tokenize_column(df, 'body')
    df = _remove_duplicate_entries(df,'title')
    df = _drop_missing_values(df)
    _save_data(df,filename)

    return df

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
    logger.info('Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]

    logger.info('Nespaper uid detected: {}'.format(newspaper_uid))

    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Adding newspaper uid column: {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df

def _extract_host(df):
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df

def _generate_uids_for_rows(df):
    logger.info('Generating uids for each row')

    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest()) )
    df['uid']= uids

    df.set_index('uid', inplace=True)

    return df

def _remove_new_lines_from_body(df):
    logger.info('Removing new lines from body')

    df['body'] = (df['body'].apply(lambda body: body.replace('\n',' ') )           )

    return df

def _tokenize_column(df, column_name):
    logger.info('Adding {}_tokens column'.format(column_name))
    df['n_tokens_{}'.format(column_name)]= (df
            .dropna()
            .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
            .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
            .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
            .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
            .apply(lambda valid_word_list: len(valid_word_list))
    )
    return df

def _remove_duplicate_entries(df,column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset = 'title', keep = 'first', inplace = True)
    return df

def _drop_missing_values(df):
    logger.info('Dropping rows with missing values')
    return df.dropna()

def _save_data(df,filename):
    clean_filename = 'clean_{}'.format(filename)
    logger.info('Saving data at: {}'.format(clean_filename))
    df.to_csv(clean_filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='The path to the dirty data',type=str)

    arg = parser.parse_args()
    df = main(arg.filename)
    print(df[['title','n_tokens_title','n_tokens_body']])