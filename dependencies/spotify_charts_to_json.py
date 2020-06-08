import datetime
import requests
from bs4 import BeautifulSoup
import json
import pathlib
from smart_open import open

two_days_ago = (datetime.date.today() - datetime.timedelta(days=2))
two_days_ago_for_website = two_days_ago.strftime('%Y-%m-%d')
two_days_ago_for_file = two_days_ago.strftime('%Y%m%d')

IS_GS = True

def get_web_html(website): #boilerplate for parse html 
    r = requests.get(website)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup

def get_path(path, data_type):
    if data_type == 'music_charts':
        file_name = f'music_chart_{two_days_ago_for_file}.json'
        sink = path.joinpath('music_charts', file_name)
    elif data_type == 'countries':
        file_name = 'countries.json'
        sink = path.joinpath('countries', file_name)
    return 'gs:/' + str(sink) if IS_GS else str(sink)

def write_to_file(text, sink):
    result = json.dumps(text)
    file = open(sink, 'w')
    file.write(result)
    file.close

def create_json(path):
    # get all countries
    countries = {}
    source = get_web_html('https://spotifycharts.com')
    for section in source.find_all(attrs={"data-type": "country"}): 
        for item in section.find_all('li'):
            countries[item['data-value']] = item.contents[0]
    print(countries)
    list_of_countries = [*countries] # get the keys

    # get the music chart from each country
    music_chart = {}
    for country in list_of_countries:
        current_page = f'https://spotifycharts.com/regional/{country}/daily/{two_days_ago_for_website}'
        source_for_each_country = get_web_html(current_page)
        try: # some countries does not have chart
            chart = source_for_each_country.find(class_ = 'chart-table').find('tbody')
            music_chart[country] = {}
            for song in chart.find_all('tr'):
                position = str(song.find(class_ = 'chart-table-position').contents[0])
                name = str(song.find(class_ = 'chart-table-track').find('strong').contents[0])
                artist = str(song.find(class_ = 'chart-table-track').find('span').contents[0])
                streams = str(song.find(class_ = 'chart-table-streams').contents[0]).replace(",", "")
                music_chart[country][position] = {
                    'name' : name,
                    'artist' : artist,
                    'streams': streams
                }
        except:
            print(f'error with {country}')

    # write results to files
    write_to_file(music_chart, get_path(path,'music_charts'))
    write_to_file(countries, get_path(path,'countries'))

    #print(music_chart)

if __name__ == "__main__":
    IS_GS = False
    create_json(pathlib.Path('./source_files/output_raw'))