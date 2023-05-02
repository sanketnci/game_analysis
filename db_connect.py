import csv
import mysql.connector
from datetime import datetime
import pandas as pd
import os


class DBConnect:

    def __init__(self, data_path='', host_name="localhost", user_name="sanket", password="root", database="game_data"):
        """

         DBConnect Class is responsible for all create, update & insert / DML DDL Operations required for the
         process

        """
        self.mydb = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=password,
            database=database
        )
        self.cursor = self.mydb.cursor()
        self.data_path = data_path

    def create_table_games_data_hltb(self):
        """

        Function creates games_data_hltb in the 'game_data' database

        """
        self.cursor.execute("CREATE TABLE games_data_hltb (id INT AUTO_INCREMENT PRIMARY KEY, game_name VARCHAR(255), "
                            "steam_app_id INT NULL, release_date DATE, genres VARCHAR(255), review_score INT)")
        self.mydb.commit()

    def insert_into_games_data_hltb_table(self):
        """

        Function inserts data to 'games_data_hltb' table from the source data in csv format

        """
        with open(self.data_path + '/dataset.csv', errors='replace') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)

            # Loop through each row in the CSV file and insert into the MySQL database
            for row in reader:
                release_date = datetime.strptime(row[2], '%d/%m/%Y').strftime('%Y-%m-%d')
                steam_app_id = int(row[1]) if row[1] else None
                cursor = self.mydb.cursor()
                cursor.execute(
                    "INSERT INTO games_data_hltb (game_name, steam_app_id, release_date, genres, review_score) VALUES \
                        ( % s, % s, % s, % s, % s)", (row[0], steam_app_id, release_date, row[3], row[4]))
            self.mydb.commit()

    def create_dlc_table(self):
        """
        Function for dlc table creation

        """
        self.cursor.execute(
            "CREATE TABLE dlc(steam_app_id INT NULL, dlc_name VARCHAR(255), polled VARCHAR(255), main VARCHAR(10), "
            "main_plus VARCHAR(10), hundred_percent_completion VARCHAR(10), game_name VARCHAR(255))")
        self.mydb.commit()

    def view_games_table(self):
        """
        Function for table viewing

        """
        self.cursor.execute("SELECT * FROM games_data_hltb")
        rows = self.cursor.fetchall()
        for i in rows:
            print(i)

    def insert_data_into_dlc(self, df):
        """
        Function for inserting dlc details to "dlc" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing DlC Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO dlc (steam_app_id, dlc_name, polled, main, main_plus, " \
                        "hundred_percent_completion, game_name) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            values = (row['steam_app_id'] if not pd.isna(row['steam_app_id']) else '0', row['DLC_Name'], row['Polled']
                      , row['Main'], row['Main_plus'], row['hundred'], row['game_name'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()

    def create_speedruns_table(self):
        """
        Function for speedruns table creation

        """
        self.cursor.execute("CREATE TABLE speedruns(steam_app_id INT NULL, game_name VARCHAR(255), speedrun_type "
                            "VARCHAR(255), polled VARCHAR(10), average VARCHAR(10), median VARCHAR(10), "
                            "fastest VARCHAR(10), slowest VARCHAR(10))")
        self.mydb.commit()

    def insert_data_into_speedruns(self, df):
        """
        Function for inserting speedruns details to "speedruns" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing speedruns Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO speedruns (steam_app_id, game_name, speedrun_type, polled, average, median, " \
                        "fastest, slowest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            values = (row['steam_app_id'] if not pd.isna(row['steam_app_id']) else '0', row['game_name']
                      , row['speedrun_type'], row['polled'], row['average'], row['median'], row['fastest']
                      , row['slowest'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()

    def create_single_player_completions_table(self):
        """
        Function for single_player_completions table creation

        """
        self.cursor.execute("CREATE TABLE single_player_completions (steam_app_id INT NULL, game_name VARCHAR(255), "
                            "single_player_completion_type VARCHAR(255), polled VARCHAR(25), average VARCHAR(25), "
                            "median VARCHAR(25), rushed VARCHAR(25), leisure VARCHAR(25))")
        self.mydb.commit()

    def insert_data_into_single_player_completions(self, df):
        """
        Function for inserting single_player_completions details to "single_player_completions" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing single_player_completions Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO single_player_completions (steam_app_id, game_name, " \
                        "single_player_completion_type, polled, average, median, rushed, leisure)" \
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['steam_app_id'] if not pd.isna(row['steam_app_id']) else '0', row['game_name'],
                      row['single_player_completion_type'], row['polled'], row['average'], row['median'], row['rushed'],
                      row['leisure'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()

    def create_multi_player_completions_table(self):
        """
        Function for single_player_completions table creation

        """
        self.cursor.execute("CREATE TABLE multi_player_completions (steam_app_id INT NULL, game_name VARCHAR(255), "
                            "multi_player_completion_type VARCHAR(255), polled VARCHAR(25), average VARCHAR(25), "
                            "median VARCHAR(25), least VARCHAR(25), most VARCHAR(25))")
        self.mydb.commit()

    def insert_data_into_multi_player_completions(self, df):
        """
        Function for inserting multi_player_completions details to "multi_player_completions" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing multi_player_completions Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO multi_player_completions (steam_app_id, game_name, multi_player_completion_type, " \
                        "polled, average, median, least, most) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            values = (row['steam_app_id'] if not pd.isna(row['steam_app_id']) else '0', row['game_name'],
                      row['multi_player_completion_type'], row['polled'], row['average'], row['median'], row['least'],
                      row['most'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()

    def create_platforms_table(self):
        """
        Function for platforms table creation

        """
        self.cursor.execute("CREATE TABLE multi_player_completions (steam_app_id INT NULL, game_name VARCHAR(255), "
                            "multi_player_completion_type VARCHAR(255), polled VARCHAR(25), average VARCHAR(25), "
                            "median VARCHAR(25), least VARCHAR(25), most VARCHAR(25))")
        self.mydb.commit()

    def insert_data_into_platforms(self, df):
        """
        Function for inserting platforms details to "platforms" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing platforms Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO platforms (steam_app_id, game_name, platform, polled, main, main_plus, " \
                        "hundred_percent, fastest, slowest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            values = (row['steam_app_id'] if not pd.isna(row['steam_app_id']) else '0', row['game_name']
                      , row['platform'], row['polled'], row['main'], row['main+']
                      , row['hundred_percent'], row['fastest'], row['slowest'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()

    def create_game_price_table(self):
        """
        Function for game_price table creation

        """
        self.cursor.execute("CREATE TABLE game_price(steam_app_id INT NULL"
                            ", game_price VARCHAR(255), release_date VARCHAR(255))")
        self.mydb.commit()

    def insert_data_into_game_price(self, df):
        """
        Function for inserting game_price details to "game_price" table in "games_data" database.

        Parameters
        ----------
        df: pd.DataFrame
            Data Containing game_price Details

        """
        for index, row in df.iterrows():
            sql_query = "INSERT INTO game_price (steam_app_id, game_price, release_date) VALUES (%s, %s, %s)"
            values = (row['steam_app_id'], row['game_price'], row['release_date'])
            self.cursor.execute(sql_query, values)
        self.mydb.commit()


if __name__ == '__main__':
    """dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, '/data')"""
    file_path = "./data"
    db = DBConnect(data_path=file_path)

    # HLTB
    # db.create_table_games_data_hltb()
    # db.insert_into_games_data_hltb_table()
    db.view_games_table()
    # DLC
    dlc_data = pd.read_csv(file_path + "/addtional_content.csv", sep="~", header=0)
    # db.create_dlc_table()
    # db.insert_data_into_dlc(dlc_data)

    # SpeedRuns
    speed_data = pd.read_csv(file_path + "/speedruns_content.csv", sep="~", header=0)
    # db.create_speedruns_table()
    # db.insert_data_into_speedruns(speed_data)

    # Single Player
    # single_data = pd.read_csv(file_path + "/single_player_times_content.csv", sep="~", header=0)
    # db.create_single_player_completions_table()
    # db.insert_data_into_single_player_completions(single_data)

    # Multi Player
    multi_data = pd.read_csv(file_path + "/multi_player_times_content.csv", sep="~", header=0)
    # db.create_multi_player_completions_table()
    # db.insert_data_into_multi_player_completions(multi_data)

    # Platform
    platform_data = pd.read_csv(file_path + "/platforms_content.csv", sep="~", header=0)
    # db.create_platforms_table()
    # db.insert_data_into_platforms(platform_data)

    # Price Insert
    price_data = pd.read_csv(file_path + "/price_data.csv", sep="~", header=0)
    price_data = price_data.fillna("")
    price_data = price_data[price_data['game_price'] != ""]
    price_data = price_data[price_data['release_date'] != ""]
    # db.insert_data_into_game_price(price_data)
