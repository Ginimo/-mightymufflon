# Libraries:
import json
import requests
import pandas as pd
from pandas import json_normalize
import time
import threading
from datetime import datetime
import logging
import os
from io import StringIO
import boto3

# Global variables: Here you need your acces key from your twitch developer account!
# With your acces key, the code generate an authentication token.
client_id = '0o8x1pt0yuc7z5xfcudg2riy5rsdfm'
client_secret = 'a4v2y735oeorqsgvxtoffb2ok7z20z'
body = {
    'client_id': client_id,
    'client_secret': client_secret,
    "grant_type": 'client_credentials'
}
r = requests.post('https://id.twitch.tv/oauth2/token', body)
# data output
keys = r.json();
headers = {
    'Client-ID': client_id,
    'Authorization': 'Bearer ' + keys['access_token']
}

#Include your AWS credentials in this section. The data can be load in the data lake 

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3 = boto3.client("s3",
                  region_name="us-east-1",
                  aws_access_key_id="ASIAQQFB54XAE27FZNWD",
                  aws_secret_access_key="iZJ29C+OaSinK7XLVqueh+2CcPSPhVTEDPYoy7vm",
                  aws_session_token="FwoGZXIvYXdzEF8aDKKAqTdPGjPk42m4ZSK4AQat2EECcOpnDPLfrWMpp"
                                    "h5lKsYczTTkC0MTp3peY4ExKzD0xu0bC0bR95PJtr8kvPXHYm5L8OVpPpO"
                                    "iAxmbq6acJTtH4neyHB0/dtKNbQmhEsufLH1vQQfYYvoV02GujRPt1xDJG"
                                    "RpIux2fW/cPRhg1DcGptDYV/FBDaNUzHWM5cn4sdwr5Oh61rbUfB5QeY9b"
                                    "KJCpLukNyRv/pBTGM63V+CXacIrmWIj0SlXhae4qivhwCMQmDpWco5aHSn"
                                    "AYyLU+rglCBYP6AwgrXDDXF+V5UU4O8YgM9uuRg2zD/di9fb8OM+rcxPoWlG1oXWg=="
                                    )

#Take access to the twitch API. In the first part, the data from the top 100 games will be loaded.
def get_twitch_data():
    # Top Games
    games_response = requests.get('https://api.twitch.tv/helix/games/top?first=100', headers=headers)
    games_response_json = json.loads(games_response.text)
    topgames_data = games_response_json['data']
    topgames_df = pd.DataFrame.from_dict(json_normalize(topgames_data), orient='columns')

    # Top Streamers get loaded in this part and two df will be generated. One df contains just the top games 
    # and the other df includes the Streamer and dthe games ther streaming about
    topstreamsforgame_response = requests.get(
        'https://api.twitch.tv/helix/streams?game_id=' + str(topgames_df['id'][0]) + '&first=100', headers=headers)
    topstreamsforgame_response_json = json.loads(topstreamsforgame_response.text)
    topstreamsforgame_data = topstreamsforgame_response_json['data']
    topstreamsforgame_df = pd.DataFrame.from_dict(json_normalize(topstreamsforgame_data), orient='columns')
    for i in range(1, 19):
        topstreamsforgame_response = requests.get(
            'https://api.twitch.tv/helix/streams?game_id=' + str(topgames_df['id'][i]) + '&first=25', headers=headers)
        topstreamsforgame_response_json = json.loads(topstreamsforgame_response.text)
        topstreamsforgame_data = topstreamsforgame_response_json['data']
        topstreamsforgame_df_temp = pd.DataFrame.from_dict(json_normalize(topstreamsforgame_data), orient='columns')
        frames = [topstreamsforgame_df, topstreamsforgame_df_temp]
        topstreamsforgame_df = pd.concat(frames, ignore_index=True)
        # Now that the FOR loop is exited and we have all our data, we export it into a csv

    return topgames_df, topstreamsforgame_df

# The df's will be converted in a csv file
def load_to_s3_csv(df, name, time):
    csv_buffer = StringIO()
    bucket = 'gamelake'
    streams = name + time + '.csv'
    df.to_csv(csv_buffer, header=True, index=False)
    s3.Object(bucket, streams).put(Body=csv_buffer.getvalue())

#To be more flexible the data will be formated in json file too.
def load_to_s3_json(df, name, time):
    json_buffer = StringIO()
    bucket = 'gamelakejson'
    streams = name + time + '.json'
    df.to_json(json_buffer)
    s3.Object(bucket, streams).put(Body=json_buffer.getvalue())

#The files get loaded in the s3 bucket
def concate():
    # Create list to hold DataFrames
    df_list = []
    # Creating Session With Boto3.
    response = s3_client.list_objects(
        Bucket='gamelake',
        Prefix='topstreamsforgame')
    # Get response contents
    request_files = response['Contents']
    # Iterate over each object

    for file in request_files:
        obj = s3_client.get_object(Bucket='gamelake', Key=file['Key'])

        # Read it as DataFrame
        obj_df = pd.read_csv(obj['Body'])

        # Append DataFrame to list
        df_list.append(obj_df)

    df = pd.concat(df_list)

    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_client.put_object(Bucket='gamelake', Body=csv_buf.getvalue(), Key='master/master.csv')
    print('Master file updated!')

def lambda_handler(event, context):
    # Global variables
    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")
    df1, df2 = get_twitch_data()
    load_to_s3_csv(df1, "topgames", date_time)
    load_to_s3_csv(df2, "topstreamsforgame", date_time)
    load_to_s3_json(df1, "topgames", date_time)
    load_to_s3_json(df2, "topstreamsforgame", date_time)
    concate()
