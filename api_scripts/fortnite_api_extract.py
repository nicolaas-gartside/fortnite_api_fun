# The purpose of this file is to extract my Fortnite player stats and to store in a database

import requests as rq
import os
import pandas as pd
import sqlalchemy as sa
import json

FORTNITE_ENDPOINTS = [
    'banners',
    'playlists',
    # 'cosmetics/br',
    # 'map',
    # 'shop/br',
    'stats/br/v2'
]

V1_ENDPOINTS = [
    'banners',
    'playlists',
    'map',
    'news',
    'stats/br/v2'
]

SHOP_COLUMN_NAMES_TO_BE_EXTRACTED = [
    'specialFeatured.entries',
    'daily.entries',
    'featured.entries',
]


def clean_up_column_name(column_name):
    return column_name.replace('.entries', '')


def unpack_nested_records(column_name, df):
    # TODO: Test this!!
    column_only = df[column_name]
    converted_to_json = json.loads(column_only.to_json(orient='records'))[0]
    nested_df = pd.json_normalize(converted_to_json)
    nested_df['record_type'] = clean_up_column_name(column_name)
    return nested_df


class FortniteError(Exception):
    pass


class FortniteApi:
    def __init__(self, schema_name='fortnite'):
        self.base_url = 'https://fortnite-api.com/'
        self.schema = schema_name
        if schema_name == 'fortnite':
            self.whitelist = pd.read_csv('helpers/fortnite_whitelist.csv')
        elif schema_name == 'fortnite_test':
            self.whitelist = pd.read_csv('api_scripts/helpers/fortnite_whitelist.csv')

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

    def make_columns_consistent(self, table_name, df):
        whitelist = self.whitelist[self.whitelist['table_name'] == table_name]
        all_columns = whitelist['current_column_name'].to_list()
        df_columns = df.columns.to_list()
        for column in all_columns:
            to_remove = whitelist[whitelist['current_column_name'] == column]['remove?'].values[0]
            if column in df_columns and to_remove:
                df = df.drop(column, axis=1)
            if column not in df_columns and not to_remove:
                df[column] = None
        return df

    def correct_column_data_types(self, table_name, df):
        whitelist = self.whitelist[self.whitelist['table_name'] == table_name]
        for column in df.columns:
            c_data_type = whitelist[whitelist['current_column_name'] == column]['column_data_type'].values[0]
            if c_data_type == 'text':
                df[column] = df[column].astype(str).replace('None', None)
        return df

    def tabulate_data(self, data, table_name):
        # The playlists import does not need any fancy transformations, this will broaden as I expand to other endpoints
        df = pd.json_normalize(data)
        print('Tabulating table:', table_name)
        correct_types = df
        if table_name == 'fortnite_shop':
            batch_hash = df['hash'].values[0]
            list_dfs = []
            for i in SHOP_COLUMN_NAMES_TO_BE_EXTRACTED:
                nested_df = unpack_nested_records(i, df)
                nested_df['batch_hash'] = batch_hash
                remove_unnecessary_column = nested_df.drop('newDisplayAsset.materialInstances', axis=1)
                list_dfs.append(remove_unnecessary_column)
            for i in range(len(list_dfs)):
                list_dfs[i] = self.make_columns_consistent(table_name, list_dfs[i])
            df = pd.concat(list_dfs).reset_index().drop('index', axis=1)
            correct_types = self.correct_column_data_types(table_name, df)
        return correct_types

    # Refer to api_scripts/helpers/db_connection.py for making the connection that is passed to this function
    # This function unpacks the json into a table format and sends the data to a database
    def send_to_database(self, data, conn, table_name, if_exists):
        df = self.tabulate_data(data, table_name)
        df.to_sql(table_name, conn, schema=self.schema, if_exists=if_exists, dtype=sa.types.Text)
