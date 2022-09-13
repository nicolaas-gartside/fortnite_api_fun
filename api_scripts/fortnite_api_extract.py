# The purpose of this file is to extract my Fortnite player stats and to store in a database

import requests as rq
import os
import pandas as pd


ENDPOINT_V_LOOKUP = {
    'playlists': 'v1/',
    'stats/br/v2?': 'v2/'
}


class FortniteError(Exception):
    pass


class FortniteApi:
    def __init__(self, name=None, schema_name='fortnite'):
        self.api_key = os.environ['fortnite_api_key']
        self.header = self.api_key
        self.base_url = 'https://fortnite-api.com/'
        self.name = name
        self.schema=schema_name

    def get_data(self, end_point):
        args = {
            'url': self.base_url + ENDPOINT_V_LOOKUP[end_point] + end_point,
        }
        if self.name:
            added_values = {
                'params': {'name': self.name},
                'headers': {'Authorization': self.api_key}
            }
            args.update(added_values)
        response = rq.get(**args)
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
        df.to_sql(table_name, conn, schema=self.schema, if_exists=if_exists)
