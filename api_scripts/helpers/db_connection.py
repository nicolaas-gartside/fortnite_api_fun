# This is specifically to allow for the creation of sqlalchemy engines based on the desired database connection

import sqlalchemy as sa
import os


class DatabaseSetup:
    def __init__(self, db_name: str):
        self.db_username = os.environ[f'{db_name}_username']
        self.db_pw = os.environ[f'{db_name}_password']
        self.db_port = os.environ[f'{db_name}_port']
        self.db_host = 'localhost'

    def create_eng(self):
        return sa.create_engine(f'postgresql://{self.db_username}:{self.db_pw}@{self.db_host}:{self.db_port}')