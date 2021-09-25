import getopt
import logging
import pickle
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from redis import Redis

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-dataload')
logger.setLevel(logging.INFO)

DECKS_CORRELATION_MATRIX_NPY = 'decks_correlation_matrix.npy'
DECKS_CROSS_TAB_H5 = 'decks_cross_tab.h5'


def get_redis_client(hostname: str, port: int, password: str) -> Redis:
    return Redis(host=hostname, port=port, password=password)


def populate_redis(data_path: Path, redis_client: Redis):
    logger.info('loading decks cross tab')
    decks_cross_tab_df = pd.read_hdf(data_path / DECKS_CROSS_TAB_H5, 'decks')

    logger.info('loading decks correlation matrix')
    decks_correlation_matrix_df = pd.DataFrame(np.load(str(data_path / DECKS_CORRELATION_MATRIX_NPY)))
    decks_correlation_matrix_df = decks_correlation_matrix_df.set_index(decks_cross_tab_df.columns)
    decks_correlation_matrix_df.columns = list(decks_cross_tab_df.columns)

    logger.info('populating redis')

    for i in range(len(decks_correlation_matrix_df.index)):
        recommendations = pd.DataFrame({
            'correlation': decks_correlation_matrix_df.iloc[i],
            'voodooId': decks_correlation_matrix_df.index})

        redis_client.set(decks_correlation_matrix_df.iloc[i].name, pickle.dumps(recommendations))

    logger.info(f'populating redis completed, {len(decks_correlation_matrix_df.index)} cards processed')


def usage():
    print('usage: preprocess.py [-dhnpr]')
    print('  -h: help')
    print('  -d: data path')
    print('  -n: hostname')
    print('  -p: password')
    print('  -r: port')

    sys.exit(0)


def main():
    start = datetime.now()

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:n:p:r:')
    except getopt.GetoptError as error:
        print(error)
        sys.exit(-1)

    data_path = None
    hostname = None
    port = 6379
    password = None

    for o, a in opts:
        if o == '-h':
            usage()
        elif o == '-d':
            data_path = Path(a)
        elif o == '-n':
            hostname = a
        elif o == '-p':
            password = a
        elif o == '-r':
            port = int(a)
        else:
            assert False, 'unhandled option'

    if data_path is None:
        print('must specify data path')
        sys.exit(-1)

    if not data_path.exists():
        print(f'data path: {data_path} does not exist')
        sys.exit(-1)

    if hostname is None:
        print('must specify hostname')
        sys.exit(-1)

    logger.info('voodoo populate redis launching')

    redis_client = get_redis_client(hostname, port, password)
    populate_redis(data_path, redis_client)

    elapsed = datetime.now() - start
    hours = elapsed.seconds // 3600
    minutes = elapsed.seconds // 60 % 60
    seconds = elapsed.seconds % 60
    logger.info(f'voodoo populate redis completed in {hours}h {minutes}m {seconds}s')


if __name__ == '__main__':
    main()
