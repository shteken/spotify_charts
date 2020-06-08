import json
import pandas as pd
import datetime
import pathlib
from smart_open import open

two_days_ago = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y%m%d')

IS_GS = True

def get_path(path, data_type, file_type):
    if data_type == 'music_charts':
        file_name = f'music_chart_{two_days_ago}.{file_type}'
        sink = path.joinpath('music_charts', file_name)
    elif data_type == 'countries':
        file_name = f'countries.{file_type}'
        sink = path.joinpath('countries', file_name)
    return 'gs:/' + str(sink) if IS_GS else str(sink)

def read_from_file(source):
    file = open(source,'r')
    text = file.read()
    file.close
    return json.loads(text)

def create_table(path_source, path_sink):
    #music_chart
    music_chart = read_from_file(get_path(path_source, 'music_charts', 'json'))
    #print(music_chart)
    # create a dict with a composite key instead of nested dicts
    music_chart_dict = {}
    for country, inner_dict in music_chart.items():
        for position, innerest_dict in inner_dict.items():
            music_chart_dict[(country, position)] = innerest_dict
    # print(music_chart_dict)
    df = pd.DataFrame.from_dict(music_chart_dict, orient='index')
    df.rename_axis(['country', 'position'], inplace=True)
    df.to_csv(path_or_buf = get_path(path_sink, 'music_charts', 'csv'))
    #print(df)

    #countries
    countries = read_from_file(get_path(path_source, 'countries', 'json'))
    #print(countries)
    dc = pd.DataFrame.from_dict(countries, orient='index', columns=['countryFullName'])
    dc.rename_axis(['country'], inplace=True)
    dc.to_csv(path_or_buf = get_path(path_sink, 'countries', 'csv'))

if __name__ == "__main__":
    IS_GS = False
    create_table(pathlib.Path('./source_files/output_raw'), pathlib.Path('./source_files/output_tables'))