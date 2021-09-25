import logging
import pickle
import sys

import pandas as pd

from bson import json_util
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError
from redis import Redis
from tornado import ioloop
from tornado import web

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger('voodoo-dataload')
logger.setLevel(logging.INFO)

DEFAULT_NUMBER_OF_RECOMMENDATIONS = 20
VOODOO_MONGO_DB = 'voodoo'


def get_database(hostname: str, port: str, username: str, password: str) -> Database:
    connection_string = f'mongodb://{username}:{password}@{hostname}:{port}'
    client = MongoClient(connection_string)

    try:
        client.list_database_names()
    except ServerSelectionTimeoutError:
        logger.error(f'unable to connect to host {hostname}')
        sys.exit(-1)

    return client[VOODOO_MONGO_DB]


def get_redis_client(hostname: str, port: int, password: str) -> Redis:
    return Redis(host=hostname, port=port, password=password)


class CardHandler(web.RequestHandler):
    def get(self, card_id: str = None):
        db_client = self.settings['db_client']
        card = db_client.cards.find_one({'voodooId': card_id})
        del card['_id']

        if card is None:
            self.send_error(404)
            return

        self.write(json_util.dumps(card))


class RecommendationHandler(web.RequestHandler):
    def get(self):
        db_client = self.settings['db_client']
        redis_client = self.settings['redis_client']

        try:
            card_ids = self.get_argument('card_ids')
        except web.MissingArgumentError:
            self.send_error(400, error={'error': 'no card ids provided'})
            return

        card_recommendations = []
        invalid_card_ids = []
        for card_id in [s for s in str.split(card_ids, ',') if s]:
            card_recommendations_pickle = redis_client.get(card_id)
            if card_recommendations_pickle is None:
                invalid_card_ids.append(card_id)
            else:
                card_recommendations_df = pickle.loads(card_recommendations_pickle)
                card_recommendations_df = card_recommendations_df.iloc[1:, :]

                card_recommendations.append(card_recommendations_df)

        if len(invalid_card_ids) > 0:
            error = {'error': 'invalid card ids provided', 'invalid_card_ids': invalid_card_ids}
            self.send_error(400, error=error)
            return

        merged_recommendations = pd.concat([d.set_index('voodooId') for d in card_recommendations], axis=1)
        recommendations = merged_recommendations.mean(axis=1).sort_values(ascending=False)
        recommendations = recommendations.head(DEFAULT_NUMBER_OF_RECOMMENDATIONS)

        response = {
            'cards': []
        }

        unknown_cards = []

        for i in range(len(recommendations)):
            card_id = recommendations.index[i]
            card = db_client.cards.find_one({'voodooId': card_id})
            if card is None:
                unknown_cards.append(card_id)
                name = 'UNKNOWN_CARD_NAME'
            else:
                name = card['name']
            response['cards'].append({'voodooId': card_id, 'name': name})

        if len(unknown_cards) > 0:
            response['unknown_cards'] = unknown_cards

        self.write(response)

    def write_error(self, status_code: int, **kwargs):
        self.write(kwargs['error'])


def main():
    db_client = get_database('localhost', '27017', 'mongoadmin', 'mongoadmin')
    redis_client = get_redis_client('localhost', 6379, None)
    app = web.Application([
        (r'/cards/([0-9a-fA-F-]*)', CardHandler),
        (r'/recommendations', RecommendationHandler)
    ], db_client=db_client, redis_client=redis_client)

    app.listen(8000)
    ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
