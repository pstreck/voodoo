import getopt
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-preprocess')
logger.setLevel(logging.INFO)

CARDS_EXTRACT_CSV = 'cards_extract.csv'
DECKS_EXTRACT_JSON = 'decks_extract.json'
DECKS_PREPROCESSED_CSV = 'decks_preprocessed.csv'


def preprocess(data_path: Path):
    logger.info('preprocessing started')
    logger.info('loading cards')

    cards_df = pd.read_csv(data_path / CARDS_EXTRACT_CSV)
    cards_df.rename(columns={'name': 'CardName'}, inplace=True)

    logger.info('loading decks')

    decks = []

    with open(data_path / DECKS_EXTRACT_JSON) as f:
        for line in f:
            decks.append(json.loads(line))

    decks_df = pd.DataFrame()

    logger.info('preprocessing decks')

    for deck in decks:
        deck_df = pd.concat([pd.json_normalize(deck['Mainboard']), pd.json_normalize(deck['Sideboard'])], axis=0)
        deck_df = deck_df.groupby(['CardName']).sum().reset_index()
        deck_df = pd.merge(cards_df, deck_df, how='left', on='CardName').replace(np.nan, 0)
        deck_df = deck_df.drop(['CardName'], axis=1).set_index('voodooCardId').transpose()

        decks_df = decks_df.append(deck_df, ignore_index=True)

    logger.info(f'preprocessing decks completed, {len(decks)} decks processed')

    decks_df.to_csv(data_path / DECKS_PREPROCESSED_CSV, index=False)
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
    logger.info(f'voodoo dataloader completed in {(elapsed.seconds % 3600) // 60}m {elapsed.seconds % 60}s')


if __name__ == '__main__':
    main()
