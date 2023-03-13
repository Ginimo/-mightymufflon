#####  Import  libraries
import csv
import json
from pprint import pprint
from datetime import datetime
import time

# third-party imports
import pandas as pd
import requests
import boto3


##### Fetch data

def get_request(url, parameters=None, counter=0):
    response = requests.get(url=url, params=parameters)

    if response:
        counter = 0
        return response.json()

    else:
        # response is none usually means too many requests  - Wait and Recall the function with the same url and params
        # Usually takes ca. 4 mins
        if counter < 10:
            counter = counter + 1
            print(f'{counter}. time the response was not sucessful. Wait for 10 seconds...')
            time.sleep(10)
            print(f'Try get request for URL {url} again.')
            return get_request(url, parameters=parameters, counter=counter)
        else:
            print("I tried it more than 1 times")
            pass

def get_all_games_steamspy(path=None, name=None):
    url = "https://steamspy.com/api.php"
    parameters = {"request": "all"}

    all_games = get_request(url, parameters=parameters)
    df_all_games = pd.DataFrame.from_dict(all_games, orient='index')
    df_all_games = df_all_games[['appid', 'name']].sort_values('appid').reset_index(drop=True)

    # print(df_all_games.head())
    print(df_all_games)
    print("Extraction of the 'GetAppList' finished")

    return df_all_games


def get_detailed_per_game_steam(all_games):
    #all_games = all_games[0:50]  # Limit data for testing purposes
    print("Start Extraction of the detailed information per game on Steam")
    detailed_games_list = []
    detailed_failed_games_list = []
    for index, game in all_games.iterrows():
        # print(game["appid"])
        app_id = int(game[
                         "appid"])  ##Example app id = 256897652, e.g. https://store.steampowered.com/api/appdetails?ap>        API_URL_DETAILED = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
        detailed_per_game = get_request(API_URL_DETAILED)
        try:
            success = detailed_per_game[f"{app_id}"]["success"]
            # print(success)
            if success == True:
                data_success = dict(detailed_per_game[f"{app_id}"]["data"])  ##alle daten wenn fetch erfolgreich war
                detailed_games_list.append(data_success)

            elif success == False:
                data_no_success = dict(
                    {'steam_appid': app_id})  # alle daten wenn fetch nicht erfolgreich war --> liste von allen app_ids
                detailed_failed_games_list.append(data_no_success)
        except TypeError:
            continue
    # print("List of all game app_ids where the extraction was successful:")
    # pprint(detailed_games_list)
    print("A total of :", len(detailed_games_list), "games were found with successful appid")
    # print("List of all games where the extraction was not successful:")
    # pprint(detailed_failed_games_list)
    print("A total of :", len(detailed_failed_games_list), "games were found with no successful appid")
    print("Extraction of the detailed information per game on Steam finished")
    return detailed_games_list, detailed_failed_games_list

def get_detailed_per_game_steamspy(all_games):
    'more information: "https://steamspy.com/api.php'
    print("Start Extraction of the detailed information per game on Steamspy")
    detailed_games_list2 = []
    detailed_failed_games_list2 = []
    for index, game in all_games.iterrows():
        app_id = game["appid"]  ##Example app id = 730, e.g. https://store.steampowered.com/api/appdetails?appids=730
        API_URL_DETAILED = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"
        detailed_per_game = get_request(API_URL_DETAILED)
        try:
            if detailed_per_game["name"] != "":
                data_success = dict(detailed_per_game)  ##alle daten wenn fetch erfolgreich war
                detailed_games_list2.append(data_success)

            elif detailed_per_game["name"] == "":
                data_no_success = dict(
                    {'steam_appid': app_id})  # alle daten wenn fetch nicht erfolgreich war --> liste von allen app_ids
                detailed_failed_games_list2.append(data_no_success)
        except TypeError:
            continue
    # print("List of all game app_ids where the extraction was successful:")
    # pprint(detailed_games_list2)
    print("A total of :", len(detailed_games_list2), "games were found with successful appid")
    # print("List of all games where the extraction was not successful:")
    # pprint(detailed_failed_games_list2)
    print("A total of :", len(detailed_failed_games_list2), "games were found with no successful appid")
    print("Extraction of the detailed information per game on Steamspy finished")
    return detailed_games_list2, detailed_failed_games_list2

def create_df(detailed_games_list, detailed_failed_games_list):
    df_success = pd.json_normalize(detailed_games_list)
    print("Overview of the first 10 data entries with success:")
    pprint(df_success.head(10))
    print(df_success.shape)
    print(20 * "*")

    df_no_success = pd.json_normalize(detailed_failed_games_list)
    print("Overview of the first 10 data entries with no success:")
    pprint(df_no_success.head(10))
    print(df_no_success.shape)

    return df_success, df_no_success


## Upload Data
def create_json_dumps(detailed_games_list):
    up_success = json.dumps(detailed_games_list)

    print("json dumps are ready")

    return up_success
def call_s3(up_success, name):
    #now = datetime.now()
    #date_time = now.strftime("%m%d%Y%H%M%S")
    bucket = "steam-games-raw-zone"
    # Naming of the files
    games = name  + ".json"

    s3 = boto3.client('s3')

    s3 = boto3.resource('s3',
                        aws_access_key_id='ASIA5PJSKABDSTCHPPN3',
                        aws_secret_access_key='OqqhD4SKMr+MqU87MWr/6OeJKtNFuCPGeKKe0m+u',
                        aws_session_token='FwoGZXIvYXdzEI3//////////wEaDJgDzZLbBagxY1UE2SK3AQ9j87xcshK3G4YMb2L1+qSt4HER/X3niU2qKBxhewb/gGKyln6ggIAd>

    s3.Bucket(bucket).put_object(Key=games, Body=up_success, ACL='public-read')
    print(f"The json file with the name {games} is in S3")

##  Main Function
def main():

    print(20 * "*")
    print("Start Extraction:")
    all_games = get_all_games_steamspy()
    detailed_games_list, detailed_failed_games_list = get_detailed_per_game_steam(all_games)
    #detailed_games_list2, detailed_failed_games_list2 = get_detailed_per_game_steamspy(all_games)
    print("End Extraction")
    print(20 * "*")

    print("Start Upload")
    up_success_steam = create_json_dumps(detailed_games_list)
    #up_success_steamspy = create_json_dumps(detailed_games_list2)
    call_s3 (up_success_steam,"Steam_src")
    #call_s3(up_success_steamspy, date_time, "Steamspy_src")
    print("End Upload")
    print(20 * "*")
