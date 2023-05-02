import pandas as pd
import json
import numpy as np
import csv
import ast
import mysql.connector
# from dap_project.db_connect import DBConnect

class DataProcessing:

    def __init__(self, data_path):
        """
        DataProcessing Class, has functions for accessing stats data, pre-processing it and storing actions.

        Parameters
        -------------
        data_path: str
            Path where data is stored

        """
        self.data_path = data_path

    def read_game_stats_data(self):
        """
        Following function reads json source data and returns pandas dataframe

        Returns
        ------------
        df:
            pd.DataFrame
        """
        data = []
        with open(self.data_path + '/hltb.jsonlines') as lines:
            for line in lines:
                data.append(json.loads(line))

        df = pd.DataFrame(data)
        df.to_csv(self.data_path + "/stats_dataset.csv", index=False, mode='w')
        return df

    def split_data(self, stats_data):
        """
        Following function parses the json semi structured data from the 'Stats' column in the given dataframe to form
        new files which contain parse data extracted from json.

        Parameters
        -----------
        stats_data:
            pd.DataFrame - Containing 'Stats' Column

        """

        add_cont_list = []
        sp_times_list = []
        mp_times_list = []
        platforms_list = []
        speedruns_cont_list = []
        for i, r in stats_data.iterrows():
            stats_string = r['Stats']
            stats_keys = list(stats_string.keys())
            steam_app_id = r['steam_app_id']
            game_name = r['Name']
            # print(stats_keys)

            if "Additional Content" in stats_keys:

                add_cont = stats_string["Additional Content"]
                # print(add_cont)
                for main_dict_key in add_cont:
                    # main_dict_key = x.keys()[0]
                    main_dict_value = add_cont[main_dict_key]
                    Polled = main_dict_value['Polled']
                    Rated = main_dict_value['Rated']
                    Main = main_dict_value['Main']
                    Main_plus = main_dict_value['Main+']
                    hundred = main_dict_value['100%']

                    # Create Rows
                    # TODO Change Variable Cases
                    additional_content = dict()
                    additional_content['steam_app_id'] = steam_app_id
                    additional_content['game_name'] = game_name
                    additional_content['DLC_Name'] = main_dict_key
                    additional_content['Polled'] = Polled
                    additional_content['Rated'] = Rated
                    additional_content['Main'] = Main
                    additional_content['Main_plus'] = Main_plus
                    additional_content['hundred'] = hundred
                    add_cont_list.append(additional_content)

            elif "Speedruns" in stats_string:

                speedruns = stats_string["Speedruns"]
                # print(add_cont)
                for main_dict_key in speedruns:
                    # main_dict_key = x.keys()[0]
                    main_dict_value = speedruns[main_dict_key]
                    Polled = main_dict_value['Polled']
                    Average = main_dict_value['Average']
                    Median = main_dict_value['Median']
                    Fastest = main_dict_value['Fastest']
                    Slowest = main_dict_value['Slowest']

                    # Create Rows
                    # TODO Change Variable Cases
                    speedruns_content = dict()
                    speedruns_content['steam_app_id'] = steam_app_id
                    speedruns_content['game_name'] = game_name
                    speedruns_content['speedrun_type'] = main_dict_key
                    speedruns_content['polled'] = Polled
                    speedruns_content['average'] = Average
                    speedruns_content['median'] = Median
                    speedruns_content['fastest'] = Fastest
                    speedruns_content['slowest'] = Slowest
                    speedruns_cont_list.append(speedruns_content)

            elif "Single-Player" in stats_string:

                single_player_times = stats_string["Single-Player"]
                #print(single_player_times)
                # print(add_cont)
                for main_dict_key in single_player_times:
                    # main_dict_key = x.keys()[0]
                    main_dict_value = single_player_times[main_dict_key]
                    Polled = main_dict_value['Polled']
                    Average = main_dict_value['Average']
                    Median = main_dict_value['Median']
                    Rushed = main_dict_value['Rushed']
                    Leisure = main_dict_value['Leisure']

                    # Create Rows
                    # TODO Change Variable Cases
                    single_player_times_content = dict()
                    single_player_times_content['steam_app_id'] = steam_app_id
                    single_player_times_content['game_name'] = game_name
                    single_player_times_content['single_player_completion_type'] = main_dict_key
                    single_player_times_content['polled'] = Polled
                    single_player_times_content['average'] = Average
                    single_player_times_content['median'] = Median
                    single_player_times_content['rushed'] = Rushed
                    single_player_times_content['leisure'] = Leisure
                    sp_times_list.append(single_player_times_content)

            elif "Multi-Player" in stats_string:

                multi_player_times = stats_string["Multi-Player"]
                # print(add_cont)
                for main_dict_key in multi_player_times:
                    # main_dict_key = x.keys()[0]
                    main_dict_value = multi_player_times[main_dict_key]
                    Polled = main_dict_value['Polled']
                    Average = main_dict_value['Average']
                    Median = main_dict_value['Median']
                    Least = main_dict_value['Least']
                    Most = main_dict_value['Most']

                    # Create Rows
                    # TODO Change Variable Cases
                    multi_player_times_content = dict()
                    multi_player_times_content['steam_app_id'] = steam_app_id
                    multi_player_times_content['game_name'] = game_name
                    multi_player_times_content['multi_player_completion_type'] = main_dict_key
                    multi_player_times_content['polled'] = Polled
                    multi_player_times_content['average'] = Average
                    multi_player_times_content['median'] = Median
                    multi_player_times_content['least'] = Least
                    multi_player_times_content['most'] = Most
                    mp_times_list.append(multi_player_times_content)

            elif "Platform" in stats_string:

                platforms = stats_string["Platform"]
                # print(platforms)
                # print(add_cont)
                for main_dict_key in platforms:
                    # main_dict_key = x.keys()[0]
                    main_dict_value = platforms[main_dict_key]
                    print(platforms[main_dict_key])
                    Polled = main_dict_value['Polled']
                    main = main_dict_value['Main']
                    main_plus = main_dict_value['Main +']
                    hundred_percent = main_dict_value['100%']
                    fastest = main_dict_value['Fastest']
                    slowest = main_dict_value['Slowest']

                    # Create Rows
                    # TODO Change Variable Cases
                    # TODO If needed strip the key and re assign keys for better accessing
                    platforms_content = dict()
                    platforms_content['steam_app_id'] = steam_app_id
                    platforms_content['game_name'] = game_name
                    platforms_content['platform'] = main_dict_key
                    platforms_content['polled'] = Polled
                    platforms_content['main'] = main
                    platforms_content['main+'] = main_plus
                    platforms_content['hundred_percent'] = hundred_percent
                    platforms_content['fastest'] = fastest
                    platforms_content['slowest'] = slowest
                    # print(platforms_content)
                    platforms_list.append(platforms_content)

        addtional_content_df = pd.DataFrame(add_cont_list)
        addtional_content_df = addtional_content_df.drop('Rated', axis=1)
        addtional_content_df = addtional_content_df.fillna(value=np.nan)

        speedruns_content_df = pd.DataFrame(speedruns_cont_list)
        single_player_times_content_df = pd.DataFrame(sp_times_list)
        multi_player_times_content_df = pd.DataFrame(mp_times_list)
        platforms_content_df = pd.DataFrame(platforms_list)

        addtional_content_df.to_csv(self.data_path + '/' + 'addtional_content.csv', sep='~', index=False)
        speedruns_content_df.to_csv(self.data_path + '/' + 'speedruns_content.csv', sep='~', index=False)
        single_player_times_content_df.to_csv(self.data_path + '/' + 'single_player_times_content.csv', sep='~'
                                              , index=False)
        multi_player_times_content_df.to_csv(self.data_path + '/' + 'multi_player_times_content.csv', sep='~',
                                             index=False)
        platforms_content_df.to_csv(self.data_path + '/' + 'platforms_content.csv', sep='~', index=False)


if __name__ == '__main__':
    file_path = './data'
    dp = DataProcessing(file_path)
    df = dp.read_game_stats_data()
    dp.split_data(df)
    #db = DBConnect(data_path='')
