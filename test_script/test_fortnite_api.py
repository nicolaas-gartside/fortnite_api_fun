from api_scripts.fortnite_api_extract import FortniteApi, FORTNITE_ENDPOINTS
from api_scripts.helpers.db_connection import DatabaseSetup
import subprocess as ss


def start_docker_container():
    ss.run('make d_start', shell=True)


def stop_docker_container():
    ss.run('make d_stop', shell=True)


def assert_testing():
    return True


def perform_extract(api_object, endpoint):
    data = api_object.get_data(endpoint)
    assert data
    db = DatabaseSetup('docker_postgres')
    eng = db.create_eng()
    table_name = 'fortnite_' + endpoint.replace('/br', '').replace('/v2', '')
    with eng.connect() as conn:
        conn.execute('create schema if not exists fortnite_test')
        api_object.send_to_database(data, conn, table_name, 'replace')


def test_pytest():
    assert assert_testing()


def test_fortnite_class():
    api_object = FortniteApi(schema_name='fortnite_test')
    assert api_object.base_url == 'https://fortnite-api.com/'


# This is just to ensure that docker container successfully started up and the postgres
# Database can be accessed
def test_docker_postgres_connection():
    # Start the docker container for all tests
    start_docker_container()
    db = DatabaseSetup('docker_postgres')
    eng = db.create_eng()
    eng.connect()


def test_fortnite_get_request():
    api_object = FortniteApi(schema_name='fortnite_test')
    endpoint = 'playlists'
    perform_extract(api_object, endpoint)


def test_fortnite_endpoints():
    api_object = FortniteApi(schema_name='fortnite_test')
    for endpoint in FORTNITE_ENDPOINTS:
        print(endpoint)
        perform_extract(api_object, endpoint)
    # Testing is over, turn off docker container
    stop_docker_container()
