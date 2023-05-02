import datetime
import json
import pandas as pd
from time import sleep
import requests
from bs4 import BeautifulSoup
import datefinder
import dateparser
from dagster import asset # import the `dagster` library

class SteamScrapper:

    def __init__(self, data_path):
        """
        SteamScrapper Class is used to scrape price and release data from the steam store website
        https://store.steampowered.com

        Parameters
        ----------
        data_path:
            String - Path where input and output files are stored

        """
        self.data_path = data_path
        resp = requests.get('http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json%27')
        d = json.dumps(resp.json())
        o = json.loads(d)
        games = o['applist']['apps']
        self.steam_games = games
        self.stat_games = None

    def get_game_ids(self):
        """
        get_game_ids function shares all the steam_app_ids that are present in the stats dataset for using
        as reference for scrapping data from steam store

        """
        stats = pd.read_csv(self.data_path + '/stats_dataset.csv')
        stats = stats[~stats['steam_app_id'].isnull()]
        stats['steam_app_id'] = stats['steam_app_id'].astype(int)
        stats = stats.drop_duplicates(subset=['steam_app_id'])
        stat_games = list(set(stats['steam_app_id'].tolist()))
        self.stat_games = set(stat_games)

    def crawl_data(self):
        """
        crawl_data function is a recursive function, which crawls price and release data from game specific
        steam store page with respect to stats dataset and stores data to csv

        """
        try:

            crawled_data = pd.read_csv( self.data_path + '/price_data.csv', header=0, sep='~')
            crawled_games = crawled_data['steam_app_id'].unique()
            crawled_games = set(crawled_games)
            to_crawl = list(self.stat_games.difference(crawled_games))

            for game in to_crawl:
                price_list = []
                steam_app_id = game
                if steam_app_id in self.stat_games:
                    url = f'https://store.steampowered.com/app/{game}/'

                    page = requests.get(url)
                    soup = BeautifulSoup(page.content, 'html.parser')

                    price_data = soup.find('div', class_='game_purchase_price price')
                    game_price = ""
                    if price_data:
                        game_price = price_data.text.replace('€', '').replace(',', '.').strip()

                    release_date_field = soup.find('div', class_='release_date')
                    date_string = '9999-12-31'
                    temp_date = datetime.datetime.strptime(date_string, '%Y-%m-%d')
                    release_date = None
                    if release_date_field:
                        soup = BeautifulSoup(str(release_date_field), 'html.parser')
                        release_date = soup.find_all("div", {'class': 'date'})
                        print()
                        release_date = release_date[0].text
                        rs = list(datefinder.find_dates(release_date, strict=True, base_date=temp_date))
                        if rs:
                            release_date = rs[0].date()
                        else:
                            if dateparser.parse(str(release_date)):
                                parsed_date = dateparser.parse(release_date).date()
                                parsed_date = parsed_date.replace(day=1)
                                release_date = parsed_date
                            else:
                                release_date = None

                    values = {"steam_app_id": steam_app_id,
                              "game_price": game_price,
                              "release_date": release_date}
                    print(values)
                    price_list.append(values)
                    sleep(1)

                price_df = pd.DataFrame(price_list)
                # steam_app_id~game_price~release_date
                price_df.to_csv(self.data_path + "/price_data.csv", sep="~", index=False, mode='a', header=False)
        except Exception as e:
            print(f'Exception Occured: {e}')
            sleep(300)
            self.crawl_data()

if __name__ == '__main__':
    file_path = './data'
    ss = SteamScrapper(file_path)
    ss.get_game_ids()
    ss.crawl_data()
