# The purpose of this file is to extract my Fortnite player stats and to store in a database
# Starting pre-requisites:
# TODO: Figure out how to set up a testing docker container
#   Perform using PyTest
# TODO: Include command shortcut, similar to mac make file
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
        df = pd.json_normalize(data)
        return df

    def send_to_database(self, data, conn, table_name, if_exists):
        df = self.tabulate_data(data)
        df.to_sql(table_name, conn, schema=self.schema, if_exists=if_exists)
