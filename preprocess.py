import getopt
import itertools
import json
import logging
import sys
from typing import Sequence, Iterable
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-preprocess')
logger.setLevel(logging.INFO)

CARDS_EXTRACT_CSV = 'cards_extract.csv'
DECKS_EXTRACT_JSON = 'decks_extract.json'
DECKS_PREPROCESSED_CSV = 'decks_preprocessed.csv'

BATCH_SIZE = 10000
POOL_SIZE = 8


def generate_batch(iterable: Sequence, size: int = BATCH_SIZE) -> Iterable:
    length = len(iterable)
    for index in range(0, length, size):
        yield iterable[index:min(index + size, length)]


def preprocess_deck(deck: dict, cards_df: DataFrame) -> DataFrame:
    deck_preprocessed_df = pd.concat(
        [pd.json_normalize(deck['Mainboard']), pd.json_normalize(deck['Sideboard'])], axis=0)
    deck_preprocessed_df['CardName'] = deck_preprocessed_df['CardName'].str.lower()
    deck_preprocessed_df = deck_preprocessed_df.groupby(['CardName']).sum().reset_index()
    deck_preprocessed_df = pd.merge(cards_df, deck_preprocessed_df, how='left', on='CardName').replace(np.nan, 0)
    deck_preprocessed_df = deck_preprocessed_df.drop(['CardName'], axis=1).set_index('voodooId').transpose()
    deck_preprocessed_df.insert(0, 'deckId', [deck['voodooId']])

    return deck_preprocessed_df


def preprocess(data_path: Path):
    logger.info('preprocessing started')
    logger.info('loading cards')

    cards_df = pd.read_csv(data_path / CARDS_EXTRACT_CSV)
    cards_df.rename(columns={'name': 'CardName'}, inplace=True)
    cards_df['CardName'] = cards_df['CardName'].str.lower()

    headers = cards_df.drop(['CardName'], axis=1).set_index('voodooId').transpose()
    headers.insert(0, 'deckId', [])
    headers.to_csv(data_path / DECKS_PREPROCESSED_CSV, index=False)

    logger.info('loading decks')

    decks = []

    with open(data_path / DECKS_EXTRACT_JSON) as f:
        for line in f:
            decks.append(json.loads(line))

    logger.info('preprocessing decks')

    pool = Pool(POOL_SIZE)

    for batch in generate_batch(decks):
        results = pool.starmap(preprocess_deck, zip(batch, itertools.repeat(cards_df)))
        pd.concat(results).reset_index().to_csv(data_path / DECKS_PREPROCESSED_CSV, mode='a', index=False, header=False)

    logger.info(f'preprocessing decks completed, {len(decks)} decks processed')
    logger.info(f'preprocessed decks written to {data_path / DECKS_PREPROCESSED_CSV}')
    logger.info('preprocessing completed')


def usage():
    print('usage: preprocess.py [-dh]')
    print('  -h: help')
    print('  -d: data path')

    sys.exit(0)


def main():
    start = datetime.now()

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:')
    except getopt.GetoptError as error:
        print(error)
        sys.exit(-1)

    data_path = None

    for o, a in opts:
        if o == '-h':
            usage()
        elif o == '-d':
            data_path = Path(a)
        else:
            assert False, 'unhandled option'

    if data_path is None:
        print('must specify data path')
        sys.exit(-1)

    if not data_path.exists():
        print(f'data path: {data_path} does not exist')
        sys.exit(-1)

    logger.info('voodoo preprocessor launching')

    preprocess(data_path)

    elapsed = datetime.now() - start
    hours = elapsed.seconds // 3600
    minutes = elapsed.seconds // 60 % 60
    seconds = elapsed.seconds % 60
    logger.info(f'voodoo preprocessor completed in {hours}h {minutes}m {seconds}s')


if __name__ == '__main__':
    main()
