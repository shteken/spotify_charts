import json
import pandas as pd
import datetime
from smart_open import open

def create_table():
    two_days_ago = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y%m%d')
    source = f'gs://spotify_project/output_raw/music_chart_{two_days_ago}.json'
    sink = f'gs://spotify_project/output_tables/music_chart_{two_days_ago}.csv'

    file = open(source, 'r')
    text = file.read()
    music_chart = json.loads(text)
    #print(music_chart)

    # create a dict with a composite key instead of nested dicts
    music_chart_dict = {}
    for country, inner_dict in music_chart.items():
        for position, innerest_dict in inner_dict.items():
            music_chart_dict[(country, position)] = innerest_dict
    # print(music_chart_dict)

    df = pd.DataFrame.from_dict(music_chart_dict, orient='index')
    df.rename_axis(['country', 'position'], inplace=True)
    df.to_csv(path_or_buf = sink)
    #print(df)
    return print('wrote data to gcs')