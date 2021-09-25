import getopt
import logging
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-preprocess')
logger.setLevel(logging.INFO)

DECKS_CORRELATION_MATRIX_NPY = 'decks_correlation_matrix.npy'
DECKS_CROSS_TAB_H5 = 'decks_cross_tab.h5'
DECKS_PREPROCESSED_CSV = 'decks_preprocessed.csv'


def calculate_recommendations(data_path: Path, force: bool = False):
    logger.info('calculating recommendations')

    decks_correlation_matrix_path = data_path / DECKS_CORRELATION_MATRIX_NPY
    decks_preprocessed_path = data_path / DECKS_PREPROCESSED_CSV
    decks_cross_tab_path = data_path / DECKS_CROSS_TAB_H5

    if not decks_preprocessed_path.exists():
        logger.error(f'decks preprocessed file: {decks_preprocessed_path} does not exist, aborting')
        return

    if decks_correlation_matrix_path.exists():
        logger.warning(f'decks correlation matrix file: {decks_correlation_matrix_path} exists')
        if force:
            logger.warning(f'force enabled, removing decks correlation matrix file')
            decks_correlation_matrix_path.unlink()
        else:
            logger.error('force not enabled, aborting')
            return

    if decks_cross_tab_path.exists():
        logger.warning(f'decks cross tab file: {decks_cross_tab_path} exists')
        if force:
            logger.warning(f'force enabled, removing decks cross tab file')
            decks_cross_tab_path.unlink()
        else:
            logger.error('force not enabled, aborting')
            return

    logger.info('loading deck data')
    decks_df = pd.read_csv(data_path / DECKS_PREPROCESSED_CSV)
    decks_cross_tab_df = decks_df.pivot_table(values='Count', index='deckId', columns='voodooId', fill_value=0)
    logger.info('saving decks cross tab ')
    decks_cross_tab_df.to_hdf(decks_cross_tab_path, key='decks', mode='w')

    svd = TruncatedSVD(n_components=250, random_state=5)

    logger.info('calculating deck results matrix')
    decks_results_matrix = svd.fit_transform(decks_cross_tab_df.transpose())

    logger.info('calculating deck correlation matrix')
    decks_correlation_matrix = np.corrcoef(decks_results_matrix)
    logger.info('saving deck correlation matrix')
    np.save(str(data_path / DECKS_CORRELATION_MATRIX_NPY), decks_correlation_matrix)

    logger.info('calculating recommendations completed')


def usage():
    print('usage: preprocess.py [-dh]')
    print('  -h: help')
    print('  -d: data path')
    print('  -f: force')

    sys.exit(0)


def main():
    start = datetime.now()

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hfd:')
    except getopt.GetoptError as error:
        print(error)
        sys.exit(-1)

    data_path = None
    force = False

    for o, a in opts:
        if o == '-h':
            usage()
        elif o == '-d':
            data_path = Path(a)
        elif o == '-f':
            force = True
        else:
            assert False, 'unhandled option'

    if data_path is None:
        print('must specify data path')
        sys.exit(-1)

    if not data_path.exists():
        print(f'data path: {data_path} does not exist')
        sys.exit(-1)

    logger.info('voodoo calculate recommendations launching')

    calculate_recommendations(data_path, force)

    elapsed = datetime.now() - start
    hours = elapsed.seconds // 3600
    minutes = elapsed.seconds // 60 % 60
    seconds = elapsed.seconds % 60
    logger.info(f'voodoo calculate recommendations completed in {hours}h {minutes}m {seconds}s')


if __name__ == '__main__':
    main()
