import getopt
import json
import logging
import sys
from datetime import datetime
from decimal import Decimal
from glob import glob
from pathlib import Path
from typing import Sequence, Iterable, List

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-dataload')
logger.setLevel(logging.INFO)

BATCH_SIZE = 1000

VOODOO_MONGO_DB = 'voodoo'
VOODOO_MONGO_COLLECTION_CARDS = 'cards'
VOODOO_MONGO_COLLECTION_DECKS = 'decks'
VOODOO_MONGO_COLLECTION_SETS = 'sets'
VOODOO_MONGO_COLLECTION_TOURNAMENTS = 'tournaments'

MTGJSON_ATOMIC_CARDS_FILE = 'AtomicCards.json'
MTGJSON_SET_LIST_FILE = 'SetList.json'
MTGO_DECKLIST_CACHE_PATH = 'mtgo_decklist_cache'


def convert_decimal(dict_item: object) -> object:
    if dict_item is None:
        return None

    if not isinstance(dict_item, dict):
        return dict_item

    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            convert_decimal(v)
        elif isinstance(v, list):
            for l in v:
                convert_decimal(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))

    return dict_item


def get_database(hostname: str, port: str, username: str, password: str) -> Database:
    connection_string = f'mongodb://{username}:{password}@{hostname}:{port}'
    client = MongoClient(connection_string)

    try:
        client.list_database_names()
    except ServerSelectionTimeoutError:
        logger.error(f'unable to connect to host {hostname}')
        sys.exit(-1)

    return client[VOODOO_MONGO_DB]


def generate_batch(iterable: Sequence, size: int = BATCH_SIZE) -> Iterable:
    length = len(iterable)
    for index in range(0, length, size):
        yield iterable[index:min(index + size, length)]


def process_cards(db: Database, path: Path):
    logger.info('processing cards')

    f = open(path / MTGJSON_ATOMIC_CARDS_FILE, 'r')
    cards_data = json.loads(f.read())
    f.close()

    collection = db[VOODOO_MONGO_COLLECTION_CARDS]

    cards_data_list = list(cards_data['data'].items())

    for batch in generate_batch(cards_data_list):
        operations = []
        for item in batch:
            data = item[1][0]
            operations.append(UpdateOne({'name': data['name']}, {'$set': convert_decimal(data)}, upsert=True))

        collection.bulk_write(operations)

    logger.info(f'processing cards completed, {len(cards_data_list)} cards processed')


def process_sets(db: Database, path: Path):
    logger.info('processing sets')

    f = open(path / MTGJSON_SET_LIST_FILE, 'r')
    set_data = json.loads(f.read())
    f.close()

    collection = db[VOODOO_MONGO_COLLECTION_SETS]

    for batch in generate_batch(set_data['data']):
        operations = []
        for item in batch:
            operations.append(UpdateOne({'name': item['name']}, {'$set': convert_decimal(item)}, upsert=True))

        collection.bulk_write(operations)

    logger.info(f'processing sets completed, {len(set_data["data"])} sets processed')


def process_tournaments(db: Database, path: Path):
    logger.info('processing tournaments')

    tournament_path_contents = glob(f'{path / MTGO_DECKLIST_CACHE_PATH}/*/**', recursive=True)
    tournament_files = [filename for filename in tournament_path_contents if '.json' in filename]

    collection = db[VOODOO_MONGO_COLLECTION_TOURNAMENTS]

    for batch in generate_batch(tournament_files):
        operations = []
        decks = []
        for item in batch:
            f = open(item, 'r')
            data = json.loads(f.read())
            f.close()

            operations.append(UpdateOne(
                {'Tournament.Name': data['Tournament']['Name'],
                 'Tournament.Date': data['Tournament']['Date']},
                {'$set': convert_decimal(data)}, upsert=True))
            decks = decks + data['Decks']

        process_decks(db, decks)

        collection.bulk_write(operations)

    logger.info(f'processing tournaments completed, {len(tournament_files)} tournaments processed')


def process_decks(db: Database, decks: List):
    logger.info('processing decks')

    collection = db[VOODOO_MONGO_COLLECTION_DECKS]

    for batch in generate_batch(decks):
        operations = []
        for item in batch:
            operations.append(UpdateOne({'Date': item['Date'], 'Player': item['Player'], 'Result': item['Result']},
                                        {'$set': convert_decimal(item)}, upsert=True))
        collection.bulk_write(operations)

    logger.info(f'processing decks completed, {len(decks)} decks processed')


def usage():
    print('usage: dataload.py [-dhrup]')
    print('  -h: help')
    print('  -d: data path')
    print('  -n: mongo hostname')
    print('  -r: mongo port')
    print('  -u: mongo username')
    print('  -p: mongo password')


def main():
    start = datetime.now()

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:n:r:u:p:')
    except getopt.GetoptError as error:
        print(error)
        sys.exit(-1)

    data_path = None
    hostname = None
    port = '27017'
    username = None
    password = None

    for o, a in opts:
        if o == '-h':
            usage()
        elif o == '-d':
            data_path = Path(a)
        elif o == '-n':
            hostname = a
        elif o == '-r':
            port = a
        elif o == '-u':
            username = a
        elif o == '-p':
            password = a
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

    if username is None:
        print('must specify username')
        sys.exit(-1)

    if password is None:
        print('must specify password')
        sys.exit(-1)

    logger.info('voodoo dataloader launching')

    db = get_database(hostname, port, username, password)

    process_cards(db, data_path)
    process_sets(db, data_path)
    process_tournaments(db, data_path)

    elapsed = datetime.now() - start
    logger.info(f'voodoo dataloader completed in {(elapsed.seconds % 3600) // 60}m {elapsed.seconds % 60}s')


if __name__ == '__main__':
    main()
