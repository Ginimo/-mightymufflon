#####  Import  libraries

# from pprint import pprint
import json
import csv
from datetime import datetime
import time
from io import StringIO

# third-party imports
import pandas as pd
import requests
import boto3


s3 = boto3.resource('s3',
                    aws_access_key_id='ASIA5PJSKABD7U34EAML',
                    aws_secret_access_key='0zo4ufPPNefLRvtw3VNYS47H7MlBeO+zsITIxjNq',
                    aws_session_token='FwoGZXIvYXdzEIn//////////wEaDGDpZc0xUi05L2ArOiK3Abr4F8zBHfa0f91cZXboEsuc7U98/gpSmV3/AzRRKyhClDjPcWGvWQ0fEIOAJls2sKUzldDaicY3ifQCgyxE7maYM3MCN7gySGZkYxMhHzXSQxmBXmQi2L0RQo7lXroaQga842QCxnpamm1A6GcA35g4QqeDhCsg+JLcbQKbrxp3rZTbIefEPXhtVaFX33wAjfNoTg2zAzOvskGoPt4n6puUahmY3hVZCmgpW+E3xaP6LvhEp7MFXyi2l6OcBjItnccHb/7FYbkOP0pRxF+1WuTbNb2cMnZAAVuoY+WG/RV6boWTD2bZo9Iai64D')


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
            print("I tried it more than 10 times")
            pass


def get_all_games_steamspy(path=None, name=None):
    url = "https://steamspy.com/api.php"
    parameters = {"request": "all"}

    all_games = get_request(url, parameters=parameters)
    df_all_games = pd.DataFrame.from_dict(all_games, orient='index')
    df_all_games = df_all_games[['appid', 'name']].sort_values('appid').reset_index(drop=True)

    print("Extraction of the 'GetAppList' finished")

    return df_all_games

def get_detailed_per_game_steamspy(all_games):
    'more information: "https://steamspy.com/api.php'

    # all_games = all_games [0:10] #limit data for testing purposes
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


## Upload Data
def create_json_dumps(detailed_games_list):
    up_json = json.dumps(detailed_games_list)

    print("json dumps are ready")

    return up_json


def create_csv_buffer(detailed_games_list):
    df = pd.json_normalize(detailed_games_list)
    csv_buffer = StringIO()
    up_csv = df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)

    print("csv buffer is ready")

    return csv_buffer


def call_s3_json(up, date_time, name):
    bucket = "steam-games-raw-zone"
    # Naming of the files
    games_datetime = name + date_time + ".json"
    games = name + ".json"

    s3.Bucket(bucket).put_object(Key=games_datetime, Body=up, ACL='public-read')
    s3.Bucket(bucket).put_object(Key=games, Body=up, ACL='public-read')

    print(f"The json file with the name {games_datetime} is in S3")
    print(f"The json file with the name {games} is in S3")


def call_s3_csv(up, date_time, name):
    bucket = "steam-games-raw-zone"
    # Naming of the files
    games_datetime = name + date_time + ".csv"
    games = name + ".csv"

    s3.Bucket(bucket).put_object(Key=games_datetime, Body=up.getvalue(), ACL='public-read')
    s3.Bucket(bucket).put_object(Key=games, Body=up.getvalue(), ACL='public-read')
    print(f"The csv file with the name {games_datetime} is in S3")
    print(f"The csv file with the name {games} is in S3")


##  Lamba Handler like "Main Function"

def lambda_handler(event, context):
    # Global variables
    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")

    #######
    print(20 * "*")
    print("Start Extraction:")
    all_games = get_all_games_steamspy()
    detailed_games_list2, detailed_failed_games_list2 = get_detailed_per_game_steamspy(all_games)
    print("End Extraction")
    print(20 * "*")

    print("Start Upload")
    up_json_steamspy = create_json_dumps(detailed_games_list2)
    up_csv_steamspy = create_csv_buffer(detailed_games_list2)
    call_s3_json(up_json_steamspy, date_time, "Steamspy/Steamspy_src")
    call_s3_csv(up_csv_steamspy, date_time, "Steamspy/Steamspy_src")
    print("End Upload")
    print(20 * "*")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
