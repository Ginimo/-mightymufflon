import json
import pandas as pd
import requests
from pprint import pprint
import time
from io import StringIO
from datetime import datetime
import boto3

now = datetime.now()
date_time = now.strftime("%d%m%Y%H%M%S")

s3 = boto3.client('s3')
#s3 = boto3.resource('s3')
s3 = boto3.resource('s3',
                    aws_access_key_id='ASIA5PJSKABDQZ2H3MXD',
                    aws_secret_access_key='YDjq1JIjX+dalwFMoAMZaYb42213tvgqbOWXsUOu',
                    aws_session_token='FwoGZXIvYXdzELb//////////wEaDJphKSdI1AzAwHXd4SK3AaeQjPnLjOIrksmpANhpKJol5TpvcTPNtQlsxnZJfgu/vD3PYZuMB/InQDPt87AMTqrd7fLLj4JhtAaFKiHXB5K5IghsuJakj7sDtJL1tXq46dXwKJOz2qHYnn4uAN5rBJRxL9DMVMqG8jrHD0+u5yA7a2zr5O4TrNakXRb5n505f5rV98q2dPka6wQcxALkOzg/RizKeWxn2Ff2Zc0VHBo3oP2TEoqw4G+ROi+zV3JtqA00W8Xs+iik7/SbBjItptNhv7Fsk4fZvkV+1PW5D1ZjheFB4Fjy62tAFNaC6rZ1jDKkBXuvH0Adc6EI')


df1 = pd.read_csv('C:/Users/danie/Documents/dwl/TaskScheduler/all_game_dev.csv')
name1 = "Wikipedia/Wikipedia_src" + date_time + ".csv"
name2 = "Wikipedia/Wikipedia_src" + ".csv"

df2 = pd.read_csv('C:/Users/danie/Documents/dwl/TaskScheduler/steamstore.csv', sep = "\t")
name3 = "Steamstore/Steamstore_src" + date_time + ".csv"
name4 = "Steamstore/Steamstore_src" + ".csv"

#Upload only once. Then comment it out:
df3 = pd.read_csv('C:/Users/danie/Documents/dwl/data/game_achievements.csv',encoding = 'unicode_escape')
name5 = "Game_Achievements/games_achievements_players_2018-07-01.csv"

def upload_csv_to_s3 (df, name, bucket):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)


    s3.Bucket(bucket).put_object(Key=name, Body=csv_buf.getvalue(), ACL='public-read')
    print(f"The csv file with the name {name} is in S3")

bucket = "steam-games-raw-zone"

upload_csv_to_s3 (df1, name1, bucket)
upload_csv_to_s3 (df1, name2, bucket)
upload_csv_to_s3 (df2, name3, bucket)
upload_csv_to_s3 (df2, name4,bucket)
upload_csv_to_s3 (df3, name5,bucket)