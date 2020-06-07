import datetime
import requests
from bs4 import BeautifulSoup
import json
from smart_open import open

def create_json():
    two_days_ago = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    two_days_ago_for_file = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y%m%d')
    
    # get all countries
    countries = {}
    r = requests.get('https://spotifycharts.com')
    soup = BeautifulSoup(r.text, 'html.parser')
    for section in soup.find_all(attrs={"data-type": "country"}):
        for item in section.find_all('li'):
            countries[item['data-value']] = item.contents[0]

    #print(countries)
    list_of_countries = [*countries] # get the keys

    # get the music chart from each country
    music_chart = {}
    for country in list_of_countries:
        current_page = f'https://spotifycharts.com/regional/{country}/daily/{two_days_ago}'
        current_r = requests.get(current_page)
        current_soup = BeautifulSoup(current_r.text, 'html.parser')
        try: # some countries does not have chart
            chart_soup = current_soup.find(class_ = 'chart-table').find('tbody')
            music_chart[country] = {}
            for song in chart_soup.find_all('tr'):
                music_chart[country][str(song.find(class_ = 'chart-table-position').contents[0])] = {
                                        'name' : str(song.find(class_ = 'chart-table-track').find('strong').contents[0]),
                                        'artist' : str(song.find(class_ = 'chart-table-track').find('span').contents[0]),
                                        'streams': str(song.find(class_ = 'chart-table-streams').contents[0]).replace(",", "")
                }
        except:
            print(f'error with {country}')

    result = json.dumps(music_chart)
    file = open(f'gs://spotify_project/output_raw/music_chart_{two_days_ago_for_file}.json','w')
    file.write(result)
    file.close
    #print(music_chart)
    return print('wrote data to gcs')