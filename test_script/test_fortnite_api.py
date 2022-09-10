from api_scripts.fortnite_api_v2 import FortniteApi
from api_scripts.helpers.db_connection import DatabaseSetup
import subprocess as ss


def start_docker_container():
    ss.run('make d_start', shell=True)


def stop_docker_container():
    ss.run('make d_stop', shell=True)


def assert_testing():
    return True


def test_pytest():
    assert assert_testing()


def test_fortnite_class():
    api_object = FortniteApi()
    assert api_object.base_url == 'https://fortnite-api.com/'


def test_docker_postgres_connection():
    start_docker_container()
    db = DatabaseSetup('docker_postgres')
    eng = db.create_eng()
    eng.connect()
    stop_docker_container()


def test_fortnite_get_request():
    start_docker_container()
    api_object = FortniteApi(schema_name='fortnite_test')
    endpoint = 'playlists'
    data = api_object.get_data(endpoint)
    assert data
    db = DatabaseSetup('docker_postgres')
    eng = db.create_eng()
    with eng.connect() as conn:
        conn.execute('create schema if not exists fortnite_test')
        api_object.send_to_database(data, conn, 'fortnite_playlists', 'replace')
    stop_docker_container()
