import json
import requests as rq
import sqlalchemy as sa
import pandas as pd
import os


def get_longest_value_in_series(series):
    column_by_length = series.astype(str).apply(len)
    position = list(column_by_length[column_by_length == column_by_length.max()].index)[0]
    return str(series[position])


def perform_dataframe_cell_replacements(df):
    return df.replace({'NaT': None, 'nan': None, 'None': None, 'NaN': None, 'False': False, 'True': True})


def perform_replacements(string):
    return string.replace('\\', '').replace('"', "'").replace(" True,", ' \'True\',') \
        .replace(" False,", ' \'False\',').replace(" None,", ' \'None\',').replace(" True}", ' \'True\'}') \
        .replace(" False}", ' \'False\'}').replace(" None}", ' \'None\'}').replace(" '',", " 'None',").split(' ')


def make_duplicate_column_names_unique(df):
    # 'Owner' and 'owner are also considered duplicates even though pandas does not see it that way
    current_columns = df.columns
    new_columns = []
    current_lower_case = []
    duplicate_counter = {}
    for column in current_columns:
        if column.lower() in current_lower_case:
            if column.lower() not in list(duplicate_counter.keys()):
                duplicate_counter[column.lower()] = 1
            else:
                duplicate_counter[column.lower()] += 1
        else:
            new_columns.append(column)
        current_lower_case.append(column.lower())
    df.columns = new_columns
    return df


def load_json(string, original):
    try:
        json_object = json.loads(string)
    except Exception as e:
        print(f'json string \'{original}\' failed to convert, raising error now')
        raise e
    return json_object


def convert_string_json(json_string):
    new = perform_replacements(json_string)
    current_key = ''
    key_value = {}
    new_string = ''
    is_nestled_dictionary = False
    is_nestled_list = False
    is_string_value = False
    # loop and identify keys in the dictionary, if not a key, then append as a value
    for index in range(len(new)):
        value = new[index]
        if len(value) < 1:
            key_value[current_key].append(value)
        else:
            if '[' in value[:2]:
                is_nestled_list = True
            if '{' in value[:2] and index != 0:
                is_nestled_dictionary = True
            if "'" in value[:2] and len(new[index - 1]) > 0 and index + 1 != len(new) and ':' in new[index - 1][-1]:
                is_string_value = True
            if index == 0:  # first value should always be a key in a dictionary
                current_key = value
                key_value[value] = []
                # ignore nestled lists/dictionaries for now, check to make sure previous item included a comma
                # if breaks, this can be tightened to check for a quote followed by a comma to help with special cases
            elif not is_string_value and not is_nestled_list and not is_nestled_dictionary and ':' in value[
                -1] and ',' in new[index - 1][-1]:
                current_key = value
                key_value[value] = []
            else:
                key_value[current_key].append(value)
                if ']' in value:
                    is_nestled_list = False
                if '}' in value:
                    is_nestled_dictionary = False
                if index + 1 != len(new):
                    if len(value) > 3 and "'" in value[-4:] and '\':' in new[index + 1][-2:]:
                        is_string_value = False
                    elif len(value) > 2 and "'" in value[-3:] and '\':' in new[index + 1][-2:]:
                        is_string_value = False
    for key in key_value.keys():
        new_string = new_string + ' ' + key.replace("'", '"')
        if len(key_value[key]) > 0:
            if "'" in key_value[key][0] and '[' not in key_value[key][0][0] and '{' not in key_value[key][0][0]:
                value_string = key_value[key][0]
                start_position = value_string.find("'")
                key_value[key][0] = value_string[:start_position] + '"' + value_string[start_position + 1:]
                # replace end apostrophe
                end_string = key_value[key][-1]
                key_value[key][-1] = end_string[:-2] + '"' + end_string[-1:]
            elif '[' in key_value[key][0][0] or '{' in key_value[key][0][0]:
                key_value[key][0] = '"' + key_value[key][0]
                key_value[key][-1] = key_value[key][-1][:-1] + '"' + key_value[key][-1][-1:]
        new_string = new_string + ' ' + ' '.join(key_value[key])
    loadable_string = new_string.strip().strip('"').strip("'")
    if loadable_string[-3:] in '}}"' or loadable_string[-3:] in '"}}':
        loadable_string = loadable_string[:-3] + '}"}'
    json_object = load_json(loadable_string, json_string)
    return json_object


def handle_string_json(json_string):
    # This function is a middle man to unpack a json that is a list of dictionaries
    if json_string[0] == '[':
        open_curly = 0
        space_separated = json_string.split(' ')
        values = []
        fragments = []
        for index in range(len(space_separated)):
            # identify dictionaries in a list
            value = space_separated[index]
            if '{' in value:
                open_curly += value.count('{')
            if '}' in value:
                open_curly += -value.count('}')
            values.append(value)
            # if end of dictionary reached, convert to json object and continue to next dictionary
            if open_curly == 0:
                if values[0][0] == '[':
                    values[0] = values[0][1:]
                if values[-1][-1] == ']' or values[-1][-1] == ',':
                    values[-1] = values[-1][:-1]
                fragments.append(json.dumps(convert_string_json(' '.join(values).strip(']').strip('['))))
                values = []
        loadable_string = '[' + ', '.join(fragments) + ']'
        json_object = load_json(loadable_string, json_string)
        return json_object
    else:
        return convert_string_json(json_string)


def unpack_json_column(df, column_name):
    json_column = df[column_name]
    list_json = []
    for string_json in json_column:
        if isinstance(string_json, dict):  # if already a dictionary, just append
            list_json.append(string_json)
        elif isinstance(string_json, str) and '{' in string_json:
            json_to_append = handle_string_json(string_json)
            # if return value is a list of multiple values, choose the first one and move on
            list_json.append(json_to_append[0] if isinstance(json_to_append, list) else json_to_append)
        else:
            list_json.append({})
    df_to_concat = pd.DataFrame(list_json)
    current_column_names = list(df_to_concat.columns)
    new_column_names = []
    for column in current_column_names:
        new_column_names.append(f'{column_name}_{column}')  # change from 'id' to 'creator_id'
    df_to_concat.columns = new_column_names
    for column in df_to_concat:  # check to make sure everything is unpacked as much as possible
        if df_to_concat[column].dtype == 'object':
            value = get_longest_value_in_series(df_to_concat[column])
            if '{' in value and '}' in value and value.index('{') <= 5:  # If there are more columns to unpack
                df_to_concat = make_duplicate_column_names_unique(df_to_concat)
                df_to_concat = unpack_json_column(df_to_concat, column)
    concatted_df = pd.concat([df, df_to_concat], axis=1)
    final_df = concatted_df.drop(column_name, axis=1)  # drop original column now that new columns have been added
    return final_df


def check_for_string_json(df):
    new_df = df
    for index in range(len(df.columns)):
        # A string dictionary should look similar to this: "{'a': 1, 'b': 2, 'c': 1, 'd': 2}"
        column = list(df.columns)[index]
        value = get_longest_value_in_series(df[column])
        if '{' in value and '}' in value:
            print(f'Unpacking column {column} with a string json found in the first value')
            new_df = unpack_json_column(new_df, column)
            print(f'Column {column} successfully unpacked')
    return new_df.replace({'None': None})


def get_engine():
    user_name = os.getenv('POSTGRES_USERNAME')
    password = os.getenv('POSTGRES_PASSWORD')
    engine = sa.create_engine(f'postgresql://{user_name}:{password}@localhost:5432')
    return engine


def sql_query_for_tables():
    engine = get_engine()
    with engine.connect() as conn:
        new_items = pd.read_sql(
            'select items_name, items_description, items_added, items_type_value, "items_images_smallIcon", '
            'items_images_icon from new_fortnite_cosmetics', conn)
        items = pd.read_sql(
            'select id, description, "images_smallIcon", images_icon, rarity_value, type_value, '
            'added from public.fortnite_cosmetics', conn)
        print(new_items[['items_name', 'items_images_icon', 'items_type_value']].to_string())
    return new_items, items


def grab_and_push_data(end_url, table_name):
    url = f'https://fortnite-api.com/v2/{end_url}'
    response = rq.get(url)
    records = response.json()
    fortnite_df = pd.DataFrame(records.get('data'))
    clean_data = check_for_string_json(fortnite_df)
    engine = get_engine()
    with engine.connect() as conn:
        clean_data.to_sql(table_name, conn, if_exists='replace')


end_point_and_table_names = {'cosmetics/br': 'fortnite_cosmetics', 'cosmetics/br/new': 'new_fortnite_cosmetics'}
for end_point in end_point_and_table_names.keys():
    grab_and_push_data(end_point, end_point_and_table_names[end_point])
