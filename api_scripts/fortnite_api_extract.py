# The purpose of this file is to extract my Fortnite player stats and to store in a database

import requests as rq
import os
import pandas as pd
import sqlalchemy as sa

FORTNITE_ENDPOINTS = [
    'banners',
    'playlists',
    # 'cosmetics/br',
    # 'map',
    'shop/br',
    'stats/br/v2'
]

V1_ENDPOINTS = [
    'banners',
    'playlists',
    'map',
    'news',
    'stats/br/v2'
]


class FortniteError(Exception):
    pass


class FortniteApi:
    def __init__(self, name=None, schema_name='fortnite'):
        self.base_url = 'https://fortnite-api.com/'
        self.schema = schema_name

    def request_from_api(self, end_point):
        version = 'v1/' if end_point in V1_ENDPOINTS else 'v2/'
        args = {
            'url': self.base_url + version + end_point,
        }
        if end_point == 'stats/br/v2':
            fortnite_name = os.environ['fortnite_player_id']
            fortnite_api_key = os.environ['fortnite_api_key']
            added_values = {
                'params': {'name': fortnite_name},
                'headers': {'Authorization': fortnite_api_key}
            }
            args.update(added_values)
        return rq.get(**args)

    def get_data(self, end_point):
        response = self.request_from_api(end_point)
        if response.status_code != 200:
            raise FortniteError(f'Bad request: {response.text}')
        return response.json()['data']

    @staticmethod
    def tabulate_data(data):
        # The playlists import does not need any fancy transformations, this will broaden as I expand to other endpoints
        df = pd.json_normalize(data)
        return df

    # Refer to api_scripts/helpers/db_connection.py for making the connection that is passed to this function
    # This function unpacks the json into a table format and sends the data to a database
    def send_to_database(self, data, conn, table_name, if_exists):
        df = self.tabulate_data(data)
        df.to_sql(table_name, conn, schema=self.schema, if_exists=if_exists, dtype=sa.types.Text)
