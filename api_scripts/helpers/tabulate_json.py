"""
The purpose of this file is to:
- find nested json fields within a dataframe
- tabulate that field
- include back into the original dataframe and check again
"""
import json


def get_longest_value_in_series(series):
    series_by_length = series.astype(str).apply(len)
    max_length = series_by_length.max()
    max_position = series_by_length[series_by_length == max_length].index.to_list()[0]
    return series.iloc[[max_position]].to_list()[0]


def unnest_json(df, column_name):
    pass


def check_for_json(df):
    for column in df.columns:
        value = get_longest_value_in_series(df[column])
        if isinstance(value, list):
            unnest_json(df, column_name=column)