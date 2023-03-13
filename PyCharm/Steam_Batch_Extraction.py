#####  Import  libraries
import csv
import datetime as dt
import json
import os
import statistics
import time
from pprint import pprint
from datetime import datetime

# third-party imports
import numpy as np
import pandas as pd
import requests

#####  Fetch Data

def get_request(url, parameters=None):

    try:
        response = requests.get(url=url, params=parameters)
    except SSLError as s:
        print('SSL Error:', s)

        for i in range(5, 0, -1):
            print('\rWaiting... ({})'.format(i), end='')
            time.sleep(1)
        print('\rRetrying.' + ' ' * 10)

        # recusively try again
        return get_request(url, parameters)

    if response:
        return response.json()
    else:
        # response is none usually means too many requests. Wait and try again
        print('No response, waiting 10 seconds...')
        time.sleep(10)
        print('Retrying.')
        return get_request(url, parameters)


def get_all_games_steam(path,name):
    GetAppList_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    all_games = get_request(GetAppList_URL)
    all_games = all_games["applist"]["apps"]
    print("Extraction of the 'GetAppList' finished")
    df_all_games = pd.json_normalize(all_games)
    df_all_games.to_csv(f'{path}{name}.csv', index=False)
    return df_all_games

def get_all_games_steamspy(path,name):
    GetAppList_URL = "https://steamspy.com/api.php"
    parameters = {"request": "all"}

    all_games = get_request(GetAppList_URL, parameters)
    df_all_games = pd.DataFrame.from_dict(all_games, orient='index')
    df_all_games = df_all_games[['appid', 'name']].sort_values('appid').reset_index(drop=True)

    print(df_all_games.head())
    print("Extraction of the 'GetAppList' finished")

    # Export as CSV
    df_all_games.to_csv(f"{path}{name}.csv", index=False)

    return df_all_games


def get_detailed_per_game(start, stop, parser, pause,all_games):
    """Return list of data per game generated from batches.
    """
    detailed_data = []

    # iterate through each row of detailed data, confined by start and stop
    for index, row in all_games[start:stop].iterrows():
        print('Current index: {}'.format(index), end='\r')

        appid = row['appid']
        name = row['name']

        # retrive app data for a row, handled by supplied parser, and append to list
        data = parser(appid, name)
        detailed_data.append(data)

        time.sleep(pause)  # prevent overloading api with requests

    return detailed_data


def process_batches(parser, all_games, download_path, data_filename, index_filename,
                    columns, begin=0, end=-1, batchsize=100, pause=1):
    """Process app data in batches, writing directly to file.

    parser : custom function to format request
    detailed_data : dataframe of appid and name
    download_path : path to store data
    data_filename : filename to save app data
    index_filename : filename to store highest index written
    columns : column names for file

    Keyword arguments:

    begin : starting index (get from index_filename, default 0)
    end : index to finish (defaults to end of detailed_data)
    batchsize : number of apps to write in each batch (default 100)
    pause : time to wait after each api request (defualt 1)

    returns: none
    """
    print('Starting at index {}:\n'.format(begin))

    # by default, process all apps in detailed_data
    if end == -1:
        end = len(all_games) + 1

    # generate array of batch begin and end points
    batches = np.arange(begin, end, batchsize) #begin=from get_index (to continue writing in the same file), #  end and batchsize = user input
    batches = np.append(batches, end)

    apps_written = 0
    batch_times = []

    for i in range(len(batches) - 1):
        start_time = time.time() #to measure remaning time

        start = batches[i]  #to write in print out, which line was batched (starting point)
        stop = batches[i + 1] #to write in print out, which line was batched (end point)

        detailed_data = get_detailed_per_game(start, stop, parser, pause,all_games) #get detailed data per game, for differnet parser

        rel_path = os.path.join(download_path, data_filename)

        # writing app data to file
        with open(rel_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')

            for j in range(3, 0, -1):
                print("\rAbout to write data, don't stop script! ({})".format(j), end='')
                time.sleep(0.5)

            writer.writerows(detailed_data)
            print('\rExported lines {}-{} to {}.'.format(start, stop - 1, data_filename), end=' ')

        apps_written += len(detailed_data)

        idx_path = os.path.join(download_path, index_filename)

        # writing last index to file
        with open(idx_path, 'w') as f:
            index = stop
            print(index, file=f)

        # logging time taken
        end_time = time.time()
        time_taken = end_time - start_time

        batch_times.append(time_taken)
        mean_time = statistics.mean(batch_times)

        est_remaining = (len(batches) - i - 2) * mean_time

        remaining_td = dt.timedelta(seconds=round(est_remaining))
        time_td = dt.timedelta(seconds=round(time_taken))
        mean_td = dt.timedelta(seconds=round(mean_time))

        print('Batch {} time: {} (avg: {}, remaining: {})'.format(i, time_td, mean_td, remaining_td))

    print('\nProcessing batches complete. {} app_ids written'.format(apps_written))


def reset_index(download_path, index_filename):
    """Reset index in file to 0."""
    rel_path = os.path.join(download_path, index_filename)

    with open(rel_path, 'w') as f:
        print(0, file=f)


def get_index(download_path, index_filename):
    """Retrieve index from file, returning 0 if file not found."""
    try:
        rel_path = os.path.join(download_path, index_filename)

        with open(rel_path, 'r') as f:
            index = int(f.readline())

    except FileNotFoundError:
        index = 0

    return index


def prepare_data_file(download_path, filename, index, columns):
    """Create file and write headers if index is 0."""
    if index == 0:
        rel_path = os.path.join(download_path, filename)

        with open(rel_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()

def parse_steam_request(appid, name):
    """Unique parser to handle data from Steam Store API.

    Returns : json formatted data (dict-like)
    """
    url = "http://store.steampowered.com/api/appdetails/"
    parameters = {"appids": appid}

    json_data = get_request(url, parameters=parameters)
    json_app_data = json_data[str(appid)]

    if json_app_data['success']:
        data = json_app_data['data']
    else:
        data = {'name': name, 'steam_appid': appid}

    return data




def parse_steamspy_request(appid, name):
    """Parser to handle SteamSpy API data."""
    url = "https://steamspy.com/api.php"
    parameters = {"request": "appdetails", "appid": appid}

    json_data = get_request(url, parameters)
    return json_data


def main():
    ####### Global variables #######
    download_path = 'C:/Users/danie/Documents/dwl/data/'
    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")

    steam_app_data = 'steam_data.csv'
    steam_index = 'steam_index.txt'
    steamspy_data = 'steamspy_data.csv'
    steamspy_index = 'steamspy_index.txt'

    steam_columns = [
        'type', 'name', 'steam_appid', 'required_age', 'is_free', 'controller_support',
        'dlc', 'detailed_description', 'about_the_game', 'short_description', 'fullgame',
        'supported_languages', 'header_image', 'website', 'pc_requirements', 'mac_requirements',
        'linux_requirements', 'legal_notice', 'drm_notice', 'ext_user_account_notice',
        'developers', 'publishers', 'demos', 'price_overview', 'packages', 'package_groups',
        'platforms', 'metacritic', 'reviews', 'categories', 'genres', 'screenshots',
        'movies', 'recommendations', 'achievements', 'release_date', 'support_info',
        'background', 'content_descriptors'
    ]

    steamspy_columns = [
        'appid', 'name', 'developer', 'publisher', 'score_rank', 'positive',
        'negative', 'userscore', 'owners', 'average_forever', 'average_2weeks',
        'median_forever', 'median_2weeks', 'price', 'initialprice', 'discount',
        'languages', 'genre', 'ccu', 'tags'
    ]

    print(20 * "*")
    print("Start Extraction:")
    ######## All Games  Steam Spy ######
    #all_games = get_all_games_steamspy(download_path, "all_games_steamspy")


    ###### All Games Steam ######

    all_games = get_all_games_steam(download_path,"all_games_steam")
    # NOTE: With this list we will get 155.412 games, which will more than 3 days to extract. Therefore the App_List with only 1000 games is more suitable


    ######### Detailed per Game: Steam  ##########

    # Effectively restarting the entire download process. Comment out if batch shall can continue, instead of starting from begin.
    #reset_index(download_path, steam_index)

    # Retrieve last index(row number) downloaded from file
    index = get_index(download_path, steam_index)

    # Wipe or create data file and write headers if index is 0
    prepare_data_file(download_path, steam_app_data, index, steam_columns)

    # Set end and chunksize for demonstration - remove to run through entire app list
    process_batches(
        parser=parse_steam_request,
        all_games=all_games,
        download_path=download_path,
        data_filename=steam_app_data,
        index_filename=steam_index,
        columns=steam_columns,
        begin=index,
        end=len(all_games),


        batchsize=100
    )

    ######### Detailed per Game: SteamSpy  ##########

    #  Effectively restarting the entire download process. Comment out if batch shall can continue, instead of starting from begin.
    #reset_index(download_path, steamspy_index)

    # Retrieve last index(row number) downloaded from file
    index = get_index(download_path, steamspy_index)

    # Wipe data file if index is 0
    prepare_data_file(download_path, steamspy_data, index, steamspy_columns)

    process_batches(
        parser=parse_steamspy_request,
        all_games=all_games,
        download_path=download_path,
        data_filename=steamspy_data,
        index_filename=steamspy_index,
        columns=steamspy_columns,
        begin=index,
        end=len(all_games),
        batchsize=100,
        pause=0.3
    )
    print("End Extraction")

    ######### Inspection  #########

    # inspect downloaded steamspy data
    print(20 * "*")
    pd.read_csv('C:/Users/danie/Documents/dwl/data/steamspy_data.csv').head()

    # inspect downloaded data
    print(20 * "*")
    pd.read_csv('C:/Users/danie/Documents/dwl/data/steam_data.csv').head()


if __name__ == '__main__':
    main()

