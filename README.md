# fortnite_api_fun

The purpose of this is to construct code that is able to query fortnite api, extract data and push into a postgres database

This repo uses a docker container with a postgres database for running tests.

Currently extracts data from the playlists api endpoint an uploads to a postgres database.

To see how this all comes together, I suggest looking at these directories: 
- test_script/test_fortnite_api.py 
- api_scripts/fortnite_api_extract.py
- api_scripts/helpers/db_connection.py
