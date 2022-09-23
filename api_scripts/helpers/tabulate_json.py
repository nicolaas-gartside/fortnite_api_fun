"""
The purpose of this file is to:
- find nested json fields within a dataframe
- tabulate that field
- include back into the original dataframe and check again
Might not need this, will see
"""
import json
import pandas as pd


# For shops/br, need the following directories unpacked:
# data/specialFeatured/entries
# data/daily/entries
# data/featured/entries

def get_longest_value_in_series(series):
    series_by_length = series.astype(str).apply(len)
    max_length = series_by_length.max()
    max_position = series_by_length[series_by_length == max_length].index.to_list()[0]
    return series.iloc[[max_position]].to_list()[0]


def unnest_json(df, column_name):
    new_json = df.to_json(orient='records')
    new_columns = pd.json_normalize(new_json, column_name)


def check_for_json(df):
    for column in df.columns:
        value = get_longest_value_in_series(df[column])
        if isinstance(value, list):
            unnest_json(df, column_name=column)