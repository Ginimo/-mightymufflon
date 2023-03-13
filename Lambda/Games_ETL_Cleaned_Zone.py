import json
import pandas as pd
import numpy as np
import requests
import psycopg2
import logging
import boto3
import os
from io import StringIO
from botocore.exceptions import ClientError
import psycopg2.extras as extras
import sys
from pprint import pprint
import re

s3_client = boto3.client('s3')


# s3 = boto3.resource('s3',
#                   aws_access_key_id='ASIA5PJSKABDYYY52RE4',
#                  aws_secret_access_key='NMTk4a/OwD34sy0hLpnSh8VKUWGd3ckJH0qhXddCMddOlbk',
#                 aws_session_token='FwoGZXIvYXdzEGcaDC2FrVk4p7Id9UtVpiK3AWORu0xnM452xZYneKlQhZCGd0S3nirjKcDjyEJFWT5EYEmwcYffXM8e319EXDeaMVP4M9nM2rl3LoUWwXv+yoWE93ttvYVtPkLp53mJfjIKzZAKRGF/3FVuI6g+DaL/zg8mQ54L6njCMv/beZHJz5OdhHZJvhC/KGW81vEe6w9vZqrPG20PRgHSceleBCOGXDj5vLJG3/y2HyLoyLf+DtQANdhFOpQpM2IfCilhY+X4hyMTXSO04Sj4uOObBjItXRdxxTWMLYVGgGHEKPSDY1xDYV+QYol0cSjJYxXEawO0S2j88rjh3WizQ5I8')


def read_data_from_s3():
    # Define bucket-name:
    bucket = "steam-games-raw-zone"

    # Define the csv to read from s3-Bucket:
    developer = "Wikipedia/Wikipedia_src.csv"
    game_achievements = "Game_Achievements/games_achievements_players_2018-07-01.csv"
    steamstore = "Steamstore/Steamstore_src.csv"
    steam = "Steam API/Steam_src.csv"
    steamspy = "Steamspy/Steamspy_src.csv"

    # Define the responses from s3:
    response_developer = s3_client.get_object(Bucket=bucket, Key=developer)
    response_game_achievements = s3_client.get_object(Bucket=bucket, Key=game_achievements)
    response_steamstore = s3_client.get_object(Bucket=bucket, Key=steamstore)
    response_steam = s3_client.get_object(Bucket=bucket, Key=steam)
    response_steamspy = s3_client.get_object(Bucket=bucket, Key=steamspy)

    # Define status from s3:
    status_developer = response_developer.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_game_achievements = response_game_achievements.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_steamstore = response_steamstore.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_steam = response_steam.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_steamspy = response_steamspy.get("ResponseMetadata", {}).get("HTTPStatusCode")

    ############################################################################
    # Load developer
    if status_developer == 200:
        print(f"Successful S3 get_object response. Status - {status_developer}")
        df_developer = pd.read_csv(response_developer.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_developer}")

    ############################################################################
    # Load game_achievements
    if status_game_achievements == 200:
        print(f"Successful S3 get_object response. Status - {status_game_achievements}")
        df_game_achievements = pd.read_csv(response_game_achievements.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {response_game_achievements}")

    ############################################################################
    # Load Steamstore
    if status_steamstore == 200:
        print(f"Successful S3 get_object response. Status - {status_steamstore}")
        df_steamstore = pd.read_csv(response_steamstore.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_steamstore}")
    ############################################################################
    # Load Steam
    if status_steam == 200:
        print(f"Successful S3 get_object response. Status - {status_steam}")
        df_steam = pd.read_csv(response_steam.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_steam}")
    ############################################################################
    # Load Steam Spy
    if status_steamspy == 200:
        print(f"Successful S3 get_object response. Status - {status_steamspy}")
        df_steamspy = pd.read_csv(response_steamspy.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_steamspy}")

    # Return the loaded data-frames:
    return df_developer, df_game_achievements, df_steamstore, df_steam, df_steamspy


def clean_developer(df_developer):
    # Check for Duplicates
    duplicates = df_developer[df_developer.duplicated(keep=False)]
    # pprint(duplicates)

    # Change some wrong values in order to change the datatype for the established_since (string at the moment, but should be smallint)
    # Established_Since
    df_developer_cleaned = df_developer.replace(
        {"19552012": "1955", "c. 1984": "1984", "1989 (as Johnson Voorsanger Productions)": "1989",
         "1976 (as the Red Company), 2000 (as Red Entertainment)": "2000",
         "1993 (Founded as Blank, Berlyn and Co., later Eidetic)": "1993",
         "1997 (as SCE Cambridge Studio), 2013": "1997", "199820162004200220052008200220021999": "1998",
         "20021994": "1994", "20052014": "2005",
         "2012 (as division of Japan Studio)2021 (as Team Asobi)": "2012"})

    # Change wrong values for country, because they are not representing a single value
    df_developer_cleaned = df_developer_cleaned.replace(
        {"CyprusBelarus": "Cyprus", "JapanSouth Korea": "South Korea", "Japan,United States": "Japan",
         "UkraineMalta": "Malta", "United Kingdom (England)": "United Kingdom",
         "United StatesCanada": "United States", "United StatesIndiaUnited KingdomCanada": "United States"})

    # Change wrong values for City
    df_developer_cleaned = df_developer_cleaned.replace(
        {"New York CityBangaloreLeedsLincolnLondonBallardvaleEdinburghCarlsbadOakville": "New York",
         "Tokyo MidtownMinato": "Toyko", "TokyoSeoul": "Seoul",
         "Tokyo (SIE, Inc.)San Mateo (SIE, LLC)": "San Mateo",
         "New YorkKarnatakaEnglandScotlandMassachusettsCaliforniaOntario": "New York"})

    # Remove wrong values and additions which merely represent the legal form of Developer Studio
    df_developer_cleaned = df_developer_cleaned.replace(
        {"Amazon Game Studios, Orange County (formerly Double Helix Games)": "Amazon Games Orange County",
         "Bandai Namco EntertainmentBandai Namco Studios": "Bandai Namco Studios",
         "Rockstar GamesRockstar IndiaRockstar LeedsRockstar LincolnRockstar LondonRockstar New EnglandRockstar NorthRockstar San DiegoRockstar Toronto": "Rockstar Games"})

    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].map(
        lambda n: str(n).partition("[")[0])  # alles vor dem [ sollen stehen bleiben
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].map(
        lambda n: str(n).partition("(")[0])  # alles vor dem ( sollen stehen bleiben

    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(",Ltd.", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(", Ltd.", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("Ltd.", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("Ltd", "", regex=False,
                                                                                                case=False)

    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(", LLC,", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(", LLC", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(",LLC", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("LLC", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("LLC.", "", regex=False,
                                                                                                case=False)

    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("GmbH", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(",Co.", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("Co.", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(",Inc", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(",Inc.", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(", Inc.", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace(", Inc", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("Inc.,", "",
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("Inc.", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('S.A.', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('SA', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('S.L.', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('A/S', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('S.r.l.', '',
                                                                                                regex=False, case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('SRL', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('GbR', '', regex=True,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("™", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace("®", "", regex=False,
                                                                                                case=False)
    df_developer_cleaned["Developer_Name"] = df_developer_cleaned["Developer_Name"].str.replace('"', '', regex=False,
                                                                                                case=False)
    df_developer_cleaned['Developer_Name'] = df_developer_cleaned['Developer_Name'].str.strip()

    # Check for Missing Values
    # print("Check for Missing Values")
    # print("The following columns have missing values", df_developer_cleaned.columns[df_developer_cleaned.isna().any(0)].tolist())

    # Transformation
    # No Transformation needed

    # Drop Duplicates again after Cleaning
    df_developer_cleaned = df_developer_cleaned.drop_duplicates(subset=["Developer_Name"])

    return df_developer_cleaned


def clean_game_achievements(df_game_achievements):
    ## Rename according to the database model
    df_game_achievements_cleaned = df_game_achievements.rename(
        columns={" Players Estimate ": "Owners_Estimation_GameAchievements", "Steam App ID": "Steam_App_ID"})

    # Transformation

    df_game_achievements_cleaned['Steam_App_ID'] = df_game_achievements_cleaned['Title'].str.split('"').str[2]
    df_game_achievements_cleaned['Owners_Estimation_GameAchievements'] = \
    df_game_achievements_cleaned['Title'].str.split('"').str[1]
    df_game_achievements_cleaned['Title'] = df_game_achievements_cleaned['Title'].str.split('"').str[0]

    df_game_achievements_cleaned["Title"] = df_game_achievements_cleaned["Title"].str.replace(",", "", regex=False,
                                                                                              case=False)
    df_game_achievements_cleaned["Owners_Estimation_GameAchievements"] = df_game_achievements_cleaned[
        "Owners_Estimation_GameAchievements"].str.replace(",", "", regex=False, case=False)
    df_game_achievements_cleaned["Steam_App_ID"] = df_game_achievements_cleaned["Steam_App_ID"].str.replace(",", "",
                                                                                                            regex=False,
                                                                                                            case=False)

    # List of values to remove
    values = [" Collection 102 817820", " PLUS Iris in "]

    # df_game_achievements_cleaned = df_game_achievements_cleaned[df_game_achievements_cleaned.Steam_App_ID != "PLUS Iris in"] #single
    df_game_achievements_cleaned = df_game_achievements_cleaned[
        df_game_achievements_cleaned.Steam_App_ID.isin(values) == False]  # multiple

    values2 = ["_¡ü»Ä", "Fire"]
    df_game_achievements_cleaned = df_game_achievements_cleaned[
        df_game_achievements_cleaned.Owners_Estimation_GameAchievements.isin(values2) == False]  # multiple

    # Change Data Type

    df_game_achievements_cleaned["Steam_App_ID"] = pd.to_numeric(df_game_achievements_cleaned["Steam_App_ID"])
    df_game_achievements_cleaned["Owners_Estimation_GameAchievements"] = pd.to_numeric(
        df_game_achievements_cleaned["Owners_Estimation_GameAchievements"])

    # Check for Duplicates
    # print("Check for duplicates:")
    duplicates = df_game_achievements_cleaned[df_game_achievements_cleaned.duplicated(keep=False)]
    # pprint(duplicates)

    # Drop Duplicates
    df_game_achievements_cleaned = df_game_achievements_cleaned.drop_duplicates()
    df_game_achievements_cleaned = df_game_achievements_cleaned.drop_duplicates(subset=["Steam_App_ID"])
    df_game_achievements_cleaned = df_game_achievements_cleaned.drop_duplicates(subset=["Title"])

    # Check for Missing Values
    # print("Check for Missing Values")
    # pprint(pd.isnull(df_game_achievements_cleaned))
    # print("The following columns have missing values", df_game_achievements_cleaned.columns[df_game_achievements_cleaned.isna().any(0)].tolist())
    # NaN_values_developers = df_steam_cleaned[df_steam_cleaned["developers"].isna()]

    return df_game_achievements_cleaned


def clean_steamstore(df_steamstore):
    # Drop all unnecessary columns

    df_steamstore_cleaned = df_steamstore.drop(
        columns=['title1', 'pos_ratings', 'neg_ratings'])  # normalization of the data

    ## Rename according to the database model

    df_steamstore_cleaned = df_steamstore_cleaned.rename(
        columns={"pos_ratings_perc": "Positivity", "tot_ratings": "Total_Ratings", "revenue": "Revenue_Gamestats",
                 "developer": "developers",
                 "publisher": "publishers"})

    ## Create new columns (Calculating Boxleiter method)
    df_steamstore_cleaned['Boxleiter_Number'] = 50
    df_steamstore_cleaned['Revenue_Boxleiter'] = df_steamstore_cleaned['Boxleiter_Number'] * df_steamstore_cleaned[
        "Total_Ratings"]

    # Check for Duplicates
    duplicates = df_steamstore_cleaned[df_steamstore_cleaned.duplicated(keep=False)]
    # pprint(duplicates)

    # Drop Duplicates
    df_steamstore_cleaned = df_steamstore_cleaned.drop_duplicates()

    # Missing Values
    # print("Check for Missing Values")
    # print("The following columns have missing values", df_steamstore_cleaned.columns[df_steamstore_cleaned.isna().any(0)].tolist())

    # Transformation

    # Genres
    df_steamstore_cleaned['genre1'] = df_steamstore_cleaned['genres'].str.split(',').str[0]
    df_steamstore_cleaned['genre2'] = df_steamstore_cleaned['genres'].str.split(',').str[1]
    df_steamstore_cleaned['genre3'] = df_steamstore_cleaned['genres'].str.split(',').str[2]
    df_steamstore_cleaned['genre2'] = df_steamstore_cleaned['genre2'].str.lstrip()
    df_steamstore_cleaned['genre3'] = df_steamstore_cleaned['genre3'].str.lstrip()

    # Developers
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(",Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(", Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("Ltd", "", regex=False,
                                                                                          case=False)

    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(", LLC,", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(", LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(",LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("LLC.", "", regex=False,
                                                                                          case=False)

    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("GmbH", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(",Co.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("Co.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(",Inc", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(",Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(", Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace(", Inc", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("Inc.,", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('S.A.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('SA', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('S.L.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('A/S', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('S.r.l.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('SRL', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('GbR', '', regex=True,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("™", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace("®", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["developers"] = df_steamstore_cleaned["developers"].str.replace('"', '', regex=False,
                                                                                          case=False)

    df_steamstore_cleaned['developer1'] = df_steamstore_cleaned['developers'].str.split(',').str[0]
    df_steamstore_cleaned['developer2'] = df_steamstore_cleaned['developers'].str.split(',').str[1]
    df_steamstore_cleaned['developer3'] = df_steamstore_cleaned['developers'].str.split(',').str[2]

    df_steamstore_cleaned['developer1'] = df_steamstore_cleaned['developer1'].str.strip()
    df_steamstore_cleaned['developer2'] = df_steamstore_cleaned['developer2'].str.strip()
    df_steamstore_cleaned['developer3'] = df_steamstore_cleaned['developer3'].str.strip()

    df_steamstore_cleaned["developer2"] = df_steamstore_cleaned["developer2"].str.replace('Feral Interactive (Linux)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["developer2"] = df_steamstore_cleaned["developer2"].str.replace('Feral Interactive (Mac)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["developer2"] = df_steamstore_cleaned["developer2"].str.replace(
        'Feral Interactive (Mac/Linux)', 'Feral Interactive', regex=False, case=False)
    df_steamstore_cleaned["developer3"] = df_steamstore_cleaned["developer3"].str.replace('Feral Interactive (Mac)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["developer3"] = df_steamstore_cleaned["developer3"].str.replace('Feral Interactive (Linux)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)

    df_steamstore_cleaned["developer2"] = df_steamstore_cleaned["developer2"].str.replace('Aspyr (Mac)', 'Aspyr',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["developer3"] = df_steamstore_cleaned["developer3"].str.replace('Aspyr (Linux)', 'Aspyr',
                                                                                          regex=False, case=False)

    # Publisher
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(",Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(", Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("Ltd.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("Ltd", "", regex=False,
                                                                                          case=False)

    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(", LLC,", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(", LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(",LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("LLC", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("LLC.", "", regex=False,
                                                                                          case=False)

    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("GmbH", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(",Co.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("Co.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(",Inc", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(",Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(", Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace(", Inc", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("Inc.,", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("Inc.", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('S.A.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('SA', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('S.L.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('A/S', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('S.r.l.', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('SRL', '', regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('GbR', '', regex=True,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("™", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace("®", "", regex=False,
                                                                                          case=False)
    df_steamstore_cleaned["publishers"] = df_steamstore_cleaned["publishers"].str.replace('"', '', regex=False,
                                                                                          case=False)

    df_steamstore_cleaned['publisher1'] = df_steamstore_cleaned['publishers'].str.split(',').str[0]
    df_steamstore_cleaned['publisher2'] = df_steamstore_cleaned['publishers'].str.split(',').str[1]
    df_steamstore_cleaned['publisher3'] = df_steamstore_cleaned['publishers'].str.split(',').str[2]

    df_steamstore_cleaned['publisher1'] = df_steamstore_cleaned['publisher1'].str.strip()
    df_steamstore_cleaned['publisher2'] = df_steamstore_cleaned['publisher2'].str.strip()
    df_steamstore_cleaned['publisher3'] = df_steamstore_cleaned['publisher3'].str.strip()

    df_steamstore_cleaned["publisher2"] = df_steamstore_cleaned["publisher2"].str.replace('Feral Interactive (Linux)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["publisher2"] = df_steamstore_cleaned["publisher2"].str.replace('Feral Interactive (Mac)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["publisher2"] = df_steamstore_cleaned["publisher2"].str.replace(
        'Feral Interactive (Mac/Linux)', 'Feral Interactive', regex=False, case=False)
    df_steamstore_cleaned["publisher3"] = df_steamstore_cleaned["publisher3"].str.replace('Feral Interactive (Mac)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["publisher3"] = df_steamstore_cleaned["publisher3"].str.replace('Feral Interactive (Linux)',
                                                                                          'Feral Interactive',
                                                                                          regex=False, case=False)

    df_steamstore_cleaned["publisher2"] = df_steamstore_cleaned["publisher2"].str.replace('Aspyr (Mac)', 'Aspyr',
                                                                                          regex=False, case=False)
    df_steamstore_cleaned["publisher3"] = df_steamstore_cleaned["publisher3"].str.replace('Aspyr (Linux)', 'Aspyr',
                                                                                          regex=False, case=False)

    # Franchise
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(",Ltd.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(", Ltd.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("Ltd.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("Ltd", "", regex=False,
                                                                                        case=False)

    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(", LLC,", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(", LLC", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(",LLC", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("LLC", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("LLC.", "", regex=False,
                                                                                        case=False)

    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("GmbH", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(",Co.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("Co.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(",Inc", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(",Inc.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(", Inc.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace(", Inc", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("Inc.,", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("Inc.", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('S.A.', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('SA', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('S.L.', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('A/S', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('S.r.l.', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('SRL', '', regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('GbR', '', regex=True,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("™", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace("®", "", regex=False,
                                                                                        case=False)
    df_steamstore_cleaned["franchise"] = df_steamstore_cleaned["franchise"].str.replace('"', '', regex=False,
                                                                                        case=False)

    df_steamstore_cleaned['franchise1'] = df_steamstore_cleaned['franchise'].str.split(',').str[0]
    df_steamstore_cleaned['franchise2'] = df_steamstore_cleaned['franchise'].str.split(',').str[1]
    df_steamstore_cleaned['franchise3'] = df_steamstore_cleaned['franchise'].str.split(',').str[2]

    df_steamstore_cleaned['franchise2'] = df_steamstore_cleaned['franchise2'].str.strip()
    df_steamstore_cleaned['franchise3'] = df_steamstore_cleaned['franchise3'].str.strip()

    # Titles

    df_steamstore_cleaned["title"] = df_steamstore_cleaned["title"].str.replace("™", "", regex=False, case=False)
    df_steamstore_cleaned["title"] = df_steamstore_cleaned["title"].str.replace("®", "", regex=False, case=False)
    df_steamstore_cleaned["title"] = df_steamstore_cleaned["title"].str.replace("Warhammer 40,000", "Warhammer 40000",
                                                                                regex=False, case=False)

    df_steamstore_cleaned['title'] = df_steamstore_cleaned['title'].str.strip()

    # Steam_App_ID
    # Converting the link which contains the Steam_App_ID

    steam_app_id_link_list = df_steamstore_cleaned["link"].values.tolist()
    steam_app_id_list = []

    for link in steam_app_id_link_list:
        steam_app_id = link[35:]
        steam_app_id = steam_app_id.partition("/")[0]
        steam_app_id_list.append(steam_app_id)

    df_steamstore_cleaned['steam_app_id'] = steam_app_id_list

    # Make booleans for merge

    action_bool = []
    adventure_bool = []
    action_adventure_bool = []

    genre_check = df_steamstore_cleaned[["genre1", "genre2"]]

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Action" and game2 != "Adventure") or (game1 != "Adventure" and game2 == "Action"):
            action_bool.append(True)  # 1
        else:
            action_bool.append(False)  # 0

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 != "Action") or (game1 != "Action" and game2 == "Adventure"):
            adventure_bool.append(True)
        else:
            adventure_bool.append(False)

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 == "Action") or (game1 == "Action" and game2 == "Adventure"):
            action_adventure_bool.append(True)
        else:
            action_adventure_bool.append(False)

    df_steamstore_cleaned["action"] = action_bool
    df_steamstore_cleaned["adventure"] = adventure_bool
    df_steamstore_cleaned["action_and_adventure"] = action_adventure_bool

    # Make boolean for has_another_developer

    developer2_check = df_steamstore_cleaned["developer2"].values.tolist()
    has_developer2_bool = []

    for game in developer2_check:
        if pd.isna(game) == True:
            has_developer2_bool.append(False)
        else:
            has_developer2_bool.append(True)

    df_steamstore_cleaned["has_another_developer"] = has_developer2_bool

    # Make boolean for has_publisher

    publisher_check = df_steamstore_cleaned["publishers"].values.tolist()
    has_publisher_bool = []

    for game in publisher_check:
        if pd.isna(game) == True:
            has_publisher_bool.append(False)
        else:
            has_publisher_bool.append(True)

    df_steamstore_cleaned["has_publisher"] = has_publisher_bool

    # Drop Duplicates again after Cleaning
    df_steamstore_cleaned = df_steamstore_cleaned.drop_duplicates(subset=["title"])
    df_steamstore_cleaned = df_steamstore_cleaned.drop_duplicates(subset=["steam_app_id"])

    # pprint(list(df_steamstore_cleaned.columns))

    return df_steamstore_cleaned


def clean_steam(df_steam):
    # print(30*"*")
    # print("Cleaning Steam")
    # pprint(list(df.columns))

    # Drop all unnecessary columns

    df_steam_cleaned = df_steam.drop(
        columns=['about_the_game', 'short_description', 'header_image', 'website', 'packages', 'screenshots',
                 'background', 'background_raw', 'mac_requirements.minimum', 'linux_requirements.minimum',
                 'price_overview.currency',
                 'price_overview.initial_formatted', 'price_overview.final_formatted', 'platforms.windows',
                 'platforms.mac', 'platforms.linux', 'metacritic.url',
                 'release_date.coming_soon', 'support_info.url', 'support_info.email', 'content_descriptors.ids',
                 'content_descriptors.notes', 'package_groups', 'mac_requirements',
                 'linux_requirements', 'demos', 'movies', 'achievements.total', 'achievements.highlighted',
                 'pc_requirements.recommended', 'controller_support',
                 'reviews', 'legal_notice', 'linux_requirements.recommended', 'mac_requirements.recommended',
                 'ext_user_account_notice', 'drm_notice', 'pc_requirements'])

    ## Rename according to the database model
    df_steam_cleaned = df_steam_cleaned.rename(
        columns={"type": "Game_Type", "name": "title", "steam_appid": "steam_app_id",
                 "supported_languages": "languages",
                 "dlc": "dlc_steam_app_id", "categories": "tags", "pc_requirements.minimum": "PC_Requirements_Minimum",
                 "price_overview.discount_percent": "Discount", "price_overview.final": "Current_Price",
                 "price_overview.initial": "Initial_Price",
                 "metacritic.score": "Metacritic_Score", "recommendations.total": "Total_Nr_Recommendations",
                 "release_date.date": "Release_Date"})

    # Check for Duplicates
    duplicates = df_steam_cleaned[df_steam_cleaned.duplicated(keep=False)]
    # pprint(duplicates)

    # Drop Duplicates
    df_steam_cleaned = df_steam_cleaned.drop_duplicates()

    # Check for Missing Values
    # print("Check for Missing Values")
    # print(pd.isnull(df_steam_cleaned))
    # print("The following columns have missing values", df_steam_cleaned.columns[df_steam_cleaned.isna().any(0)].tolist())
    # NaN_values_developers = df_steam_cleaned[df_steam_cleaned["developers"].isna()]

    # Fill NAs:

    df_steam_cleaned = df_steam_cleaned.fillna({'developers': "Sixense"})

    # Divide all prices with 100
    df_steam_cleaned["Current_Price"] = df_steam_cleaned["Current_Price"] / 100
    df_steam_cleaned["Initial_Price"] = df_steam_cleaned["Initial_Price"] / 100

    ## Create new columns (Calculating Boxleiter method)
    df_steam_cleaned['Boxleiter_Number'] = 50

    # Transformation

    # dlc_steam_app_id
    df_steam_cleaned["dlc_steam_app_id"] = df_steam_cleaned["dlc_steam_app_id"].str.replace("[", "", regex=False,
                                                                                            case=False)
    df_steam_cleaned["dlc_steam_app_id"] = df_steam_cleaned["dlc_steam_app_id"].str.replace("]", "", regex=False,
                                                                                            case=False)

    # Developers
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("[", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("]", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("'", "", regex=False, case=False)

    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(",Ltd.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(", Ltd.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("Ltd.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("Ltd", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(", LLC,", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(", LLC", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(",LLC", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("LLC", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("LLC.", "", regex=False, case=False)

    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("GmbH", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(",Co.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("Co.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(",Inc", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(",Inc.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(", Inc.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace(", Inc", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("Inc.,", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("Inc.", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('S.A.', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('SA', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('S.L.', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('A/S', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('S.r.l.', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('SRL', '', regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('GbR', '', regex=True, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("™", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace("®", "", regex=False, case=False)
    df_steam_cleaned["developers"] = df_steam_cleaned["developers"].str.replace('"', '', regex=False, case=False)

    df_steam_cleaned['developer1'] = df_steam_cleaned['developers'].str.split(',').str[0]
    df_steam_cleaned['developer2'] = df_steam_cleaned['developers'].str.split(',').str[1]
    df_steam_cleaned['developer3'] = df_steam_cleaned['developers'].str.split(',').str[2]

    df_steam_cleaned['developer1'] = df_steam_cleaned['developer1'].str.strip()
    df_steam_cleaned['developer2'] = df_steam_cleaned['developer2'].str.strip()
    df_steam_cleaned['developer3'] = df_steam_cleaned['developer3'].str.strip()

    df_steam_cleaned["developer2"] = df_steam_cleaned["developer2"].str.replace('Feral Interactive (Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["developer2"] = df_steam_cleaned["developer2"].str.replace('Feral Interactive (Mac)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["developer2"] = df_steam_cleaned["developer2"].str.replace('Feral Interactive (Mac/Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["developer3"] = df_steam_cleaned["developer3"].str.replace('Feral Interactive (Mac)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["developer3"] = df_steam_cleaned["developer3"].str.replace('Feral Interactive (Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)

    df_steam_cleaned["developer2"] = df_steam_cleaned["developer2"].str.replace('Aspyr (Mac)', 'Aspyr', regex=False,
                                                                                case=False)
    df_steam_cleaned["developer3"] = df_steam_cleaned["developer3"].str.replace('Aspyr (Linux)', 'Aspyr', regex=False,
                                                                                case=False)

    # Publisher
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("[", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("]", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("'", "", regex=False, case=False)

    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(",Ltd.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(", Ltd.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("Ltd.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("Ltd", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(", LLC,", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(", LLC", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(",LLC", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("LLC", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("LLC.", "", regex=False, case=False)

    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("GmbH", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(",Co.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("Co.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(",Inc", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(",Inc.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(", Inc.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace(", Inc", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("Inc.,", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("Inc.", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('S.A.', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('SA', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('S.L.', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('A/S', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('S.r.l.', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('SRL', '', regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('GbR', '', regex=True, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("™", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace("®", "", regex=False, case=False)
    df_steam_cleaned["publishers"] = df_steam_cleaned["publishers"].str.replace('"', '', regex=False, case=False)

    df_steam_cleaned['publisher1'] = df_steam_cleaned['publishers'].str.split(',').str[0]
    df_steam_cleaned['publisher2'] = df_steam_cleaned['publishers'].str.split(',').str[1]
    df_steam_cleaned['publisher3'] = df_steam_cleaned['publishers'].str.split(',').str[2]

    df_steam_cleaned['publisher1'] = df_steam_cleaned['publisher1'].str.strip()
    df_steam_cleaned['publisher2'] = df_steam_cleaned['publisher2'].str.strip()
    df_steam_cleaned['publisher3'] = df_steam_cleaned['publisher3'].str.strip()

    df_steam_cleaned["publisher2"] = df_steam_cleaned["publisher2"].str.replace('Feral Interactive (Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["publisher2"] = df_steam_cleaned["publisher2"].str.replace('Feral Interactive (Mac)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["publisher2"] = df_steam_cleaned["publisher2"].str.replace('Feral Interactive (Mac/Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["publisher3"] = df_steam_cleaned["publisher3"].str.replace('Feral Interactive (Mac)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)
    df_steam_cleaned["publisher3"] = df_steam_cleaned["publisher3"].str.replace('Feral Interactive (Linux)',
                                                                                'Feral Interactive', regex=False,
                                                                                case=False)

    df_steam_cleaned["publisher2"] = df_steam_cleaned["publisher2"].str.replace('Aspyr (Mac)', 'Aspyr', regex=False,
                                                                                case=False)
    df_steam_cleaned["publisher3"] = df_steam_cleaned["publisher3"].str.replace('Aspyr (Linux)', 'Aspyr', regex=False,
                                                                                case=False)

    # Tags
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace("[", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace("]", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace("{", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace("}", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace("'", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace(" description: ", "", regex=False, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace(r"id: [0-9],", "", regex=True, case=False)
    df_steam_cleaned["tags"] = df_steam_cleaned["tags"].str.replace(r"id: [0-9][0-9],", "", regex=True, case=False)

    df_steam_cleaned['tag1'] = df_steam_cleaned['tags'].str.split(',').str[0]
    df_steam_cleaned['tag2'] = df_steam_cleaned['tags'].str.split(',').str[1]
    df_steam_cleaned['tag3'] = df_steam_cleaned['tags'].str.split(',').str[2]
    df_steam_cleaned['tag1'] = df_steam_cleaned['tag1'].str.strip()
    df_steam_cleaned['tag2'] = df_steam_cleaned['tag2'].str.strip()
    df_steam_cleaned['tag3'] = df_steam_cleaned['tag3'].str.strip()

    # Genres
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace("[", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace("]", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace("{", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace("}", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace("'", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace(" description: ", "", regex=False, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace(r"id: [0-9],", "", regex=True, case=False)
    df_steam_cleaned["genres"] = df_steam_cleaned["genres"].str.replace(r"id: [0-9][0-9],", "", regex=True, case=False)

    df_steam_cleaned['genre1'] = df_steam_cleaned['genres'].str.split(',').str[0]
    df_steam_cleaned['genre2'] = df_steam_cleaned['genres'].str.split(',').str[1]
    df_steam_cleaned['genre3'] = df_steam_cleaned['genres'].str.split(',').str[2]
    df_steam_cleaned['genre1'] = df_steam_cleaned['genre1'].str.strip()
    df_steam_cleaned['genre2'] = df_steam_cleaned['genre2'].str.strip()
    df_steam_cleaned['genre3'] = df_steam_cleaned['genre3'].str.strip()

    # PC Requirements Minimum
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("<br>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("</br>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("<br />", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("<li>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("</li>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("<p>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("</p>", "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("<strong>",
                                                                                                          "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace("</strong>",
                                                                                                          "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace(
        '<ul class="bb_ul">', "", regex=False, case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace('</ul>', "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace(
        '<h2 class="bb_tag">', "", regex=False, case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace('</h2>', "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace('\r', "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace('\t', "",
                                                                                                          regex=False,
                                                                                                          case=False)
    df_steam_cleaned["PC_Requirements_Minimum"] = df_steam_cleaned["PC_Requirements_Minimum"].str.replace('\n', "",
                                                                                                          regex=False,
                                                                                                          case=False)

    # Detailed Description
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<br>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</br>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<br />", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<li>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</li>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<p>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</p>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<strong>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</strong>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace(
        '<ul class="bb_ul">', "", regex=False, case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace('</ul>', "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace(
        '<h2 class="bb_tag">', "", regex=False, case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace('</h2>', "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<a href=", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<img src=", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<h1>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</h1>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<i>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</i>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("<a>", "",
                                                                                                    regex=False,
                                                                                                    case=False)
    df_steam_cleaned["detailed_description"] = df_steam_cleaned["detailed_description"].str.replace("</a>", "",
                                                                                                    regex=False,
                                                                                                    case=False)

    # Languages
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<br>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</br>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<br />", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<li>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</li>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<p>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</p>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<strong>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</strong>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace('<ul class="bb_ul">', "", regex=False,
                                                                              case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace('</ul>', "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace('<h2 class="bb_tag">', "", regex=False,
                                                                              case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace('</h2>', "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<a href=", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<img src=", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<h1>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</h1>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<i>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</i>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("<a>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("</a>", "", regex=False, case=False)
    df_steam_cleaned["languages"] = df_steam_cleaned["languages"].str.replace("*", " ", regex=False, case=False)

    # Release_Date
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.strip()
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("2013 年 8 月 15 日", "Aug-2013",
                                                                                    regex=False, case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("2020 年 6 月 4 日", "Jun-2020",
                                                                                    regex=False, case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("2022 年 4 月 21 日", "Apr-2022",
                                                                                    regex=False, case=False)

    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("янв", "Jan", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("фев", "Feb", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("мар", "Mar", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("Апр", "Mar", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("мая", "May", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("mai", "May", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("июн", "Jun", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("Июль", "Jul", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("авг", "Aug", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("сен", "Sep", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("Окт", "Oct", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("ноя", "Nov", regex=False,
                                                                                    case=False)
    df_steam_cleaned["Release_Date"] = df_steam_cleaned["Release_Date"].str.replace("дек", "Dec", regex=False,
                                                                                    case=False)

    # release_date_list = df_steam_cleaned["Release_Date"].values.tolist()
    df_steam_cleaned = df_steam_cleaned.fillna({'Release_Date': "No Date"})
    release_date_list = list(df_steam_cleaned["Release_Date"])
    release_date_list_new = []
    my_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    for release_date in release_date_list:
        if any(substring.lower() in release_date.lower() for substring in my_list):
            release_date = release_date.lower()
            if "jan" in release_date:
                month = "01"
            elif "feb" in release_date:
                month = "02"
            elif "mar" in release_date:
                month = "03"
            elif "apr" in release_date:
                month = "04"
            elif "may" in release_date:
                month = "05"
            elif "jun" in release_date:
                month = "06"
            elif "jul" in release_date:
                month = "07"
            elif "aug" in release_date:
                month = "08"
            elif "sep" in release_date:
                month = "09"
            elif "oct" in release_date:
                month = "10"
            elif "nov" in release_date:
                month = "11"
            elif "dec" in release_date:
                month = "12"
            else:
                month = "Any"
            year = str(release_date[-4:])
            release_date_new = year + month + "01"
        else:
            release_date_new = ""
        release_date_list_new.append(release_date_new)
    df_steam_cleaned["Release_Date"] = release_date_list_new

    # Titles
    df_steam_cleaned["title"] = df_steam_cleaned["title"].str.replace("™", "", regex=False, case=False)
    df_steam_cleaned["title"] = df_steam_cleaned["title"].str.replace("®", "", regex=False, case=False)
    df_steam_cleaned["title"] = df_steam_cleaned["title"].str.replace("Warhammer 40,000", "Warhammer 40000",
                                                                      regex=False, case=False)

    df_steam_cleaned['title'] = df_steam_cleaned['title'].str.strip()

    # Make booleans for merge

    action_bool = []
    adventure_bool = []
    action_adventure_bool = []

    genre_check = df_steam_cleaned[["genre1", "genre2"]]

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Action" and game2 != "Adventure") or (game1 != "Adventure" and game2 == "Action"):
            action_bool.append(True)  # 1
        else:
            action_bool.append(False)  # 0

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 != "Action") or (game1 != "Action" and game2 == "Adventure"):
            adventure_bool.append(True)
        else:
            adventure_bool.append(False)

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 == "Action") or (game1 == "Action" and game2 == "Adventure"):
            action_adventure_bool.append(True)
        else:
            action_adventure_bool.append(False)

    df_steam_cleaned["action"] = action_bool
    df_steam_cleaned["adventure"] = adventure_bool
    df_steam_cleaned["action_and_adventure"] = action_adventure_bool

    # Make boolean for has_dlc:
    dlc_check = df_steam_cleaned["dlc_steam_app_id"].values.tolist()
    has_dlc_bool = []

    for game in dlc_check:
        if pd.isna(game) == True:
            has_dlc_bool.append(False)
        else:
            has_dlc_bool.append(True)

    df_steam_cleaned["has_dlc"] = has_dlc_bool

    # Make boolean for has_another_developer

    developer2_check = df_steam_cleaned["developer2"].values.tolist()
    has_developer2_bool = []

    for game in developer2_check:
        if pd.isna(game) == True:
            has_developer2_bool.append(False)
        else:
            has_developer2_bool.append(True)

    df_steam_cleaned["has_another_developer"] = has_developer2_bool

    # Make boolean for has_publisher

    publisher_check = df_steam_cleaned["publishers"].values.tolist()
    has_publisher_bool = []

    for game in publisher_check:
        if pd.isna(game) == True:
            has_publisher_bool.append(False)
        else:
            has_publisher_bool.append(True)

    df_steam_cleaned["has_publisher"] = has_publisher_bool

    # Drop Duplicates again after Cleaning
    df_steam_cleaned = df_steam_cleaned.drop_duplicates(subset=["steam_app_id"])
    df_steam_cleaned = df_steam_cleaned.drop_duplicates(subset=["title"])

    return df_steam_cleaned


def clean_steamspy(df_steamspy):
    # Delete last columns after column 19
    df_steamspy_cleaned = df_steamspy.iloc[:, 0:19]

    ## Rename according to the database model

    df_steamspy_cleaned = df_steamspy_cleaned.rename(columns={"appid": "steam_app_id", "name": "title",
                                                              "owners": "owners_estimation_steamspy",
                                                              "average_forever": "average_playtime_forever",
                                                              "average_2weeks": "average_playtime_2weeks",
                                                              "median_forever": "median_playtime_forever",
                                                              "median_2weeks": "median_playtime_2weeks",
                                                              "price": "current_price", "initialprice": "initial_price",
                                                              "genre": "Genres", "developer": "Developers",
                                                              "publisher": "Publishers"})

    # Divide all prices with 100
    df_steamspy_cleaned["current_price"] = df_steamspy_cleaned["current_price"] / 100
    df_steamspy_cleaned["initial_price"] = df_steamspy_cleaned["initial_price"] / 100

    ## Create new columns (Boxleiter number)
    df_steamspy_cleaned['Boxleiter_Number'] = 50

    # Check for Duplicates
    duplicates = df_steamspy_cleaned[df_steamspy_cleaned.duplicated(keep=False)]

    # Drop Duplicates
    df_steamspy_cleaned = df_steamspy_cleaned.drop_duplicates()

    # Missing Values
    # print("Check for Missing Values")
    # print("The following columns have missing values", df_steamspy_cleaned.columns[df_steamspy_cleaned.isna().any(0)].tolist())

    # Transformation

    # Genres
    df_steamspy_cleaned['genre1'] = df_steamspy_cleaned['Genres'].str.split(',').str[0]
    df_steamspy_cleaned['genre2'] = df_steamspy_cleaned['Genres'].str.split(',').str[1]
    df_steamspy_cleaned['genre3'] = df_steamspy_cleaned['Genres'].str.split(',').str[2]
    df_steamspy_cleaned['genre2'] = df_steamspy_cleaned['genre1'].str.strip()
    df_steamspy_cleaned['genre2'] = df_steamspy_cleaned['genre2'].str.strip()
    df_steamspy_cleaned['genre3'] = df_steamspy_cleaned['genre3'].str.strip()

    # Developers
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(",Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(", Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("Ltd", "", regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(", LLC,", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(", LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(",LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("LLC.", "", regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("GmbH", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(",Co.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("Co.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(",Inc", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(",Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(", Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace(", Inc", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("Inc.,", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('S.A.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('SA', '', regex=False, case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('S.L.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('A/S', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('S.r.l.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('SRL', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('GbR', '', regex=True, case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("™", "", regex=False, case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace("®", "", regex=False, case=False)
    df_steamspy_cleaned["Developers"] = df_steamspy_cleaned["Developers"].str.replace('"', '', regex=False, case=False)

    df_steamspy_cleaned['developer1'] = df_steamspy_cleaned['Developers'].str.split(',').str[0]
    df_steamspy_cleaned['developer2'] = df_steamspy_cleaned['Developers'].str.split(',').str[1]
    df_steamspy_cleaned['developer3'] = df_steamspy_cleaned['Developers'].str.split(',').str[2]

    df_steamspy_cleaned['developer1'] = df_steamspy_cleaned['developer1'].str.strip()
    df_steamspy_cleaned['developer2'] = df_steamspy_cleaned['developer2'].str.strip()
    df_steamspy_cleaned['developer3'] = df_steamspy_cleaned['developer3'].str.strip()

    df_steamspy_cleaned["developer2"] = df_steamspy_cleaned["developer2"].str.replace('Feral Interactive (Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["developer2"] = df_steamspy_cleaned["developer2"].str.replace('Feral Interactive (Mac)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["developer2"] = df_steamspy_cleaned["developer2"].str.replace('Feral Interactive (Mac/Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["developer3"] = df_steamspy_cleaned["developer3"].str.replace('Feral Interactive (Mac)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["developer3"] = df_steamspy_cleaned["developer3"].str.replace('Feral Interactive (Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["developer2"] = df_steamspy_cleaned["developer2"].str.replace('Aspyr (Mac)', 'Aspyr',
                                                                                      regex=False, case=False)
    df_steamspy_cleaned["developer3"] = df_steamspy_cleaned["developer3"].str.replace('Aspyr (Linux)', 'Aspyr',
                                                                                      regex=False, case=False)

    # Publisher
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(
        "Kakao Games Europe B.V. (EU,NA,OCE), Nimble Neuron (Excluding EU,NA,OCE)",
        'Kakao Games Europe B.V., Nimble Neurong EU', regex=False, case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(",Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(", Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("Ltd.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("Ltd", "", regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(", LLC,", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(", LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(",LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("LLC", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("LLC.", "", regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("GmbH", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(",Co.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("Co.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(",Inc", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(",Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(", Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace(", Inc", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("Inc.,", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace("Inc.", "", regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('S.A.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('SA', '', regex=False, case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('S.L.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('A/S', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('S.r.l.', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('SRL', '', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["Publishers"] = df_steamspy_cleaned["Publishers"].str.replace('GbR', '', regex=True, case=False)

    df_steamspy_cleaned['publisher1'] = df_steamspy_cleaned['Publishers'].str.split(',').str[0]
    df_steamspy_cleaned['publisher2'] = df_steamspy_cleaned['Publishers'].str.split(',').str[1]
    df_steamspy_cleaned['publisher3'] = df_steamspy_cleaned['Publishers'].str.split(',').str[2]

    df_steamspy_cleaned['publisher1'] = df_steamspy_cleaned['publisher1'].str.strip()
    df_steamspy_cleaned['publisher2'] = df_steamspy_cleaned['publisher2'].str.strip()
    df_steamspy_cleaned['publisher3'] = df_steamspy_cleaned['publisher3'].str.strip()

    df_steamspy_cleaned["publisher2"] = df_steamspy_cleaned["publisher2"].str.replace('Feral Interactive (Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["publisher2"] = df_steamspy_cleaned["publisher2"].str.replace('Feral Interactive (Mac)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["publisher2"] = df_steamspy_cleaned["publisher2"].str.replace('Feral Interactive (Mac/Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["publisher3"] = df_steamspy_cleaned["publisher3"].str.replace('Feral Interactive (Mac)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)
    df_steamspy_cleaned["publisher3"] = df_steamspy_cleaned["publisher3"].str.replace('Feral Interactive (Linux)',
                                                                                      'Feral Interactive', regex=False,
                                                                                      case=False)

    df_steamspy_cleaned["publisher2"] = df_steamspy_cleaned["publisher2"].str.replace('Aspyr (Mac)', 'Aspyr',
                                                                                      regex=False, case=False)
    df_steamspy_cleaned["publisher2"] = df_steamspy_cleaned["publisher2"].str.replace('Aspyr (Linux)', 'Aspyr',
                                                                                      regex=False, case=False)
    df_steamspy_cleaned["publisher3"] = df_steamspy_cleaned["publisher3"].str.replace('Aspyr (Linux)', 'Aspyr',
                                                                                      regex=False, case=False)
    df_steamspy_cleaned["publisher3"] = df_steamspy_cleaned["publisher3"].str.replace('Aspyr (Mac)', 'Aspyr',
                                                                                      regex=False, case=False)

    # Titles

    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("™", "", regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("®", "", regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Age of Empires IV: Anniversary Edition",
                                                                            "Age of Empires IV", regex=False,
                                                                            case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Fallout 3: Game of the Year Edition",
                                                                            "Fallout 3", regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Mortal KombatÂ 11", "Mortal Kombat 11",
                                                                            regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Sid Meierâ€™s Civilization VI",
                                                                            "Sid Meier’s Civilization VI", regex=False,
                                                                            case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Tom Clancyâ€™s Splinter Cell Blacklist",
                                                                            "Tom Clancy's Splinter Cell Blacklist",
                                                                            regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Tom Clancyâ€™s The Division",
                                                                            "Tom Clancy's The Division", regex=False,
                                                                            case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace("Total War: EMPIRE â€“ Definitive Edition",
                                                                            "Total War: EMPIRE- Definitive Edition",
                                                                            regex=False, case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace(
        "Total War: MEDIEVAL II â€“ Definitive Edition", "Total War: MEDIEVAL II  - Definitive Edition", regex=False,
        case=False)
    df_steamspy_cleaned["title"] = df_steamspy_cleaned["title"].str.replace(
        "Total War: NAPOLEON â€“ Definitive Edition", "Total War: NAPOLEON - Definitive Edition", regex=False,
        case=False)
    df_steamspy_cleaned['title'] = df_steamspy_cleaned['title'].str.strip()

    # Make booleans for merge

    action_bool = []
    adventure_bool = []
    action_adventure_bool = []

    genre_check = df_steamspy_cleaned[["genre1", "genre2"]]

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Action" and game2 != "Adventure") or (game1 != "Adventure" and game2 == "Action"):
            action_bool.append(True)  # 1
        else:
            action_bool.append(False)  # 0

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 != "Action") or (game1 != "Action" and game2 == "Adventure"):
            adventure_bool.append(True)
        else:
            adventure_bool.append(False)

    for game1, game2 in genre_check.itertuples(index=False):
        if (game1 == "Adventure" and game2 == "Action") or (game1 == "Action" and game2 == "Adventure"):
            action_adventure_bool.append(True)
        else:
            action_adventure_bool.append(False)

    df_steamspy_cleaned["action"] = action_bool
    df_steamspy_cleaned["adventure"] = adventure_bool
    df_steamspy_cleaned["action_and_adventure"] = action_adventure_bool

    # Make boolean for has_another_developer

    developer2_check = df_steamspy_cleaned["developer2"].values.tolist()
    has_developer2_bool = []

    for game in developer2_check:
        if pd.isna(game) == True:
            has_developer2_bool.append(False)
        else:
            has_developer2_bool.append(True)

    df_steamspy_cleaned["has_another_developer"] = has_developer2_bool

    # Make boolean for has_publisher

    publisher_check = df_steamspy_cleaned["Publishers"].values.tolist()
    has_publisher_bool = []

    for game in publisher_check:
        if pd.isna(game) == True:
            has_publisher_bool.append(False)
        else:
            has_publisher_bool.append(True)

    df_steamspy_cleaned["has_publisher"] = has_publisher_bool

    # Drop Duplicates again after Cleaning
    df_steamspy_cleaned = df_steamspy_cleaned.drop_duplicates(subset=["steam_app_id"])
    df_steamspy_cleaned = df_steamspy_cleaned.drop_duplicates(subset=["title"])

    return df_steamspy_cleaned


def load_developer_to_postgresql(df_developer):
    print(30 * "*")
    print("Loading Developers")
    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    df = df_developer

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Developers CASCADE"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Developers (Developer_Name VARCHAR(200) UNIQUE, City VARCHAR(100),Administrative_Division VARCHAR(100),Country VARCHAR(100),Established_Since DECIMAL, Notable_Games VARCHAR(1000),Notes VARCHAR(500), Developer_Type VARCHAR (100), Developer_ID SERIAL PRIMARY KEY)"
    table_altering_query = "ALTER TABLE developers ADD COLUMN Developed_Games_ID BIGINT"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        cursor.execute(table_altering_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "developers"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

    conn.close()


def load_game_achievements_to_postgresql(df_game_achievements):
    print(30 * "*")
    print("Loading Game_Achievements")
    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    df = df_game_achievements

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Game_Achievements"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Game_Achievements (title VARCHAR(200) UNIQUE, Owners_Estimation_GameAchievements REAL, Steam_App_ID REAL PRIMARY KEY)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "Game_Achievements"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:

        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

    conn.close()


def load_steamstore_to_postgresql(df_steamstore):
    print(30 * "*")
    print("Loading Steamstore")

    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    df = df_steamstore

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Steamstore"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Steamstore (link VARCHAR(200),title VARCHAR(500) UNIQUE, genres VARCHAR(200), developers VARCHAR(200), publishers VARCHAR (200), franchise VARCHAR (200), total_ratings real, Positivity real, link_gamestat VARCHAR(200), Revenue_Gamestats real, genre1 VARCHAR(50), genre2 VARCHAR(50),  genre3 VARCHAR(50), developer1 VARCHAR(50), developer2 VARCHAR(50), developer3 VARCHAR(50), publisher1 VARCHAR(50), publisher2 VARCHAR(50), publisher3 VARCHAR(50), franchise1 VARCHAR(50), franchise2 VARCHAR(50), franchise3 VARCHAR(50), Boxleiter_Number SMALLINT, Revenue_Boxleiter REAL, steam_app_id BIGINT PRIMARY KEY , action BOOLEAN, adventure BOOLEAN, action_and_adventure BOOLEAN, has_another_developer BOOLEAN, has_publisher BOOLEAN)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "Steamstore"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:

        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print("execute_values() done")

    # Some values has problem to be changed in pandas, because they have really a hard pattern to replace. It is much easier with SQL.
    update_query = "UPDATE steamstore SET title = 'Road 96' WHERE title LIKE 'Road 96%'"
    update_query2 = "UPDATE steamstore SET title = 'Deadside Hooligan Skin Set' WHERE title LIKE 'Deadside%Hooligan%'"
    update_query3 = "UPDATE steamstore SET title = 'Beat Saber - Kendrick Lamar - DNA.' WHERE title LIKE 'Beat Saber - Kendrick Lamar%'"
    update_query4 = "UPDATE steamstore SET title = 'Beat Saber - Linkin Park - Numb' WHERE title LIKE '%Beat Saber%Numb%'"
    update_query5 = "UPDATE steamstore SET title = 'Beat Saber - Linkin Park - One Step Closer' WHERE title LIKE '%Beat Saber%One Step Closer%'"
    update_query6 = "UPDATE steamstore SET title = 'Beat Saber - Lizzo - Cuz I Love You' WHERE title LIKE '%Beat Saber%Cuz I Love You%'"
    update_query7 = "UPDATE steamstore SET title = 'Beat Saber - One Republic - Counting Stars' WHERE title LIKE '%Beat Saber%Counting Stars%'"
    update_query8 = "UPDATE steamstore SET title = 'Beat Saber - Sid Tipton & Timbaland - Has A Meaning' WHERE title LIKE '%Beat Saber%Has A Meaning%'"
    update_query9 = "UPDATE steamstore SET title = 'Ragnarock - Gloryhammer - Gloryhammer' WHERE title LIKE '%Ragnarock%Gloryhammer%Gloryhammer%'"
    update_query10 = "UPDATE steamstore SET title = 'Ragnarock - Gloryhammer - Questlords of Inverness, Ride to the Galactic Fortress!' WHERE title LIKE '%Ragnarock%Questlords of Inverness, Ride to the Galactic Fortress!%'"
    update_query11 = "UPDATE steamstore SET title = 'Ragnarock - Gloryhammer - Rise of the Chaos Wizards' WHERE title LIKE '%Ragnarock%Rise of the Chaos Wizards%'"
    update_query12 = "UPDATE steamstore SET title = 'Ragnarock - Gloryhammer - The Land of Unicorns' WHERE title LIKE '%Ragnarock%The Land of Unicorns%'"
    update_query13 = "UPDATE steamstore SET title = 'Ragnarock - Gloryhammer - The Siege of Dunkeld (In Hoots We Trust)' WHERE title LIKE '%Ragnarock%The Siege of Dunkeld (In Hoots We Trust)%'"

    try:
        print("Updating Table")
        # Execute commands
        cursor.execute(update_query)
        cursor.execute(update_query2)
        cursor.execute(update_query3)
        cursor.execute(update_query4)
        cursor.execute(update_query5)
        cursor.execute(update_query6)
        cursor.execute(update_query7)
        cursor.execute(update_query8)
        cursor.execute(update_query9)
        cursor.execute(update_query10)
        cursor.execute(update_query11)
        cursor.execute(update_query12)
        cursor.execute(update_query13)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("update_values() done")
    cursor.close()
    conn.close()


def load_steam_to_postgresql(df_steam):
    print(30 * "*")
    print("Loading Steam")

    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    df = df_steam

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Steam"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Steam (Game_Type VARCHAR(50), Title VARCHAR(600) UNIQUE, Steam_App_ID BIGINT PRIMARY KEY, Required_Age SMALLINT, Is_Free BOOLEAN, Detailed_Description VARCHAR(100000), Languages VARCHAR(5000), Developers VARCHAR(600), Publishers VARCHAR(600), dlc_steam_app_id VARCHAR(20000), Tags VARCHAR(5000), Genres VARCHAR(5000), PC_Requirements_Minimum VARCHAR(5000), Initial_Price real, Current_Price real, Discount real, Metacritic_Score real, Total_Nr_Recommendations real, Release_Date VARCHAR(50),Boxleiter_Number real, developer1 VARCHAR(50), developer2 VARCHAR(50), developer3 VARCHAR (50), publisher1 VARCHAR(50), publisher2 VARCHAR(50), publisher3 VARCHAR (50), tag1 VARCHAR(50), tag2 VARCHAR (50), tag3 VARCHAR (50), genre1 VARCHAR(50), genre2 VARCHAR(50), genre3 VARCHAR(50), action BOOLEAN, adventure BOOLEAN, action_and_adventure BOOLEAN, has_dlc BOOLEAN, has_another_developer BOOLEAN, has_publisher BOOLEAN)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "Steam"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:

        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")

    # Some values has problem to be changed in pandas, because they have really a hard pattern to replace. It is much easier with SQL.
    update_query = "UPDATE steam SET developer1 = 'Greenheart Games' WHERE developer1 LIKE 'Greenheart Games%'"
    update_query2 = "UPDATE steam SET developer2 = 'Greenheart Games' WHERE developer2 LIKE 'Greenheart Games%'"
    update_query3 = "UPDATE steam SET developer3 = 'Greenheart Games' WHERE developer3 LIKE 'Greenheart Games%'"
    update_query4 = "UPDATE steam SET publisher1 = 'Greenheart Games' WHERE publisher1 LIKE 'Greenheart Games%'"
    update_query5 = "UPDATE steam SET publisher2 = 'Greenheart Games' WHERE publisher2 LIKE 'Greenheart Games%'"
    update_query6 = "UPDATE steam SET publisher3 = 'Greenheart Games' WHERE publisher3 LIKE 'Greenheart Games%'"
    update_query7 = "UPDATE steam SET  release_date = null\
    WHERE steam_app_id in (44350, 33900, 202170, 8980, 224540, 102500, 577800, 237110, 49540, 40960, 63380, 250820, 214560, 223850, 226700, 219740,242050)"
    altering_query1 = "ALTER TABLE steam ALTER COLUMN release_date TYPE DATE using to_date(release_date, 'YYYYMMDD');"

    try:
        print("Updating and Altering Table")
        # Execute commands
        cursor.execute(update_query)
        cursor.execute(update_query2)
        cursor.execute(update_query3)
        cursor.execute(update_query4)
        cursor.execute(update_query5)
        cursor.execute(update_query6)
        cursor.execute(update_query7)
        cursor.execute(altering_query1)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("update_values() done")
    cursor.close()

    conn.close()


def load_steamspy_to_postgresql(df_steamspy):
    print(30 * "*")
    print("Loading Steamspy")

    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    df = df_steamspy

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Steamspy"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Steamspy (Steam_App_ID INTEGER PRIMARY KEY, Title VARCHAR(200) UNIQUE, Developers VARCHAR(500), Publishers VARCHAR(200), Score_Rank real, Positive INTEGER, Negative INTEGER, Userscore INTEGER, Owners_Estimation_Steamspy VARCHAR(100), Average_Playtime_Forever INTEGER, Average_Playtime_2weeks INTEGER, Median_Playtime_Forever INTEGER, Median_Playtime_2weeks INTEGER, Current_Price REAL, Initial_Price REAL, Discount SMALLINT, CCU INTEGER, Languages VARCHAR(400), Genres VARCHAR(200), Boxleiter_Number SMALLINT, genre1 VARCHAR(100), genre2 VARCHAR(50), genre3 VARCHAR (100), developer1 VARCHAR(100), developer2 VARCHAR(100), developer3 VARCHAR(100), publisher1 VARCHAR(100), publisher2 VARCHAR(100), publisher3 VARCHAR(100), action BOOLEAN, adventure BOOLEAN, action_and_adventure BOOLEAN, has_another_developer BOOLEAN, has_publisher BOOLEAN)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "Steamspy"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:

        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

    conn.close()


def create_games_relation():
    print(30 * "*")
    print("Create  Games Relation")

    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS games_pre"
    table_creation_query = "SELECT s.steam_app_id,\
    s.title,\
    s.owners_estimation_steamspy,\
    ga.owners_estimation_gameachievements,\
    s3.revenue_gamestats,\
    s3.revenue_boxleiter,\
    s.average_playtime_forever,\
    s.average_playtime_2weeks,\
    s.median_playtime_forever,\
    s.median_playtime_2weeks,\
    s.current_price,\
    s.initial_price,\
    s.discount,\
    s.ccu,\
    s.languages,\
    s.boxleiter_number,\
    s.genre1,\
    s.genre2,\
    s.genre3, \
    s.developer1, \
    s.developer2, \
    s.developer3, \
    s.publisher1, \
    s.publisher2, \
    s.publisher3, \
    s.action, \
    s.adventure, \
    s.action_and_adventure, \
    s.has_another_developer, \
    s.has_publisher, \
    s2.required_age, \
    s2.is_free, \
    s2.detailed_description, \
    s2.dlc_steam_app_id, \
    s2.pc_requirements_minimum, \
    s2.metacritic_score, \
    s2.total_nr_recommendations, \
    s2.release_date, \
    s2.tag1, \
    s2.tag2, \
    s2.tag3, \
    s2.has_dlc,  \
    s3.total_ratings, \
    s3.positivity \
    INTO games_pre \
    FROM steamspy s  \
    LEFT JOIN game_achievements ga on s.steam_app_id = ga.steam_app_id \
    LEFT JOIN steam s2 on s.steam_app_id  = s2.steam_app_id  \
    LEFT JOIN steamstore s3 on s.steam_app_id = s3.steam_app_id"

    table_altering_query = "ALTER TABLE games_pre ADD PRIMARY KEY (steam_app_id, title)"
    table_altering_query2 = "ALTER TABLE games_pre ADD COLUMN revenue_gameachievements"

    table_updating_query_pre = """
    UPDATE games_pre gpre
    SET  revenue_gameachievements = (
    SELECT gpre.initial_price * gpre.owners_estimation_gameachievements
    FROM revenue_gameachievements gpre);"""

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        print("Altering Table")
        cursor.execute(table_altering_query)
        cursor.execute(table_altering_query2)
        cursor.execute(table_updating_query_pre)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("execute_values() done")
        print(error)

    table_delete_query2 = "DROP TABLE IF EXISTS games CASCADE"
    table_creation_query2 = "SELECT g.title, g.genre1, g.genre2, g.genre3, g.developer1, g.developer2, g.developer3, g.publisher1, g.publisher2, g.publisher3, g.total_ratings, g.positivity, g.boxleiter_number, \
    g.revenue_gamestats, g.revenue_boxleiter, g.action, g.adventure, g.action_and_adventure, g.has_another_developer, g.has_publisher, g.steam_app_id, g.owners_estimation_steamspy, g.owners_estimation_gameachievements, g.revenue_gameachievements, \
    g.average_playtime_forever, g.average_playtime_2weeks, g.median_playtime_forever, g.median_playtime_2weeks, g.current_price, g.initial_price, g.discount, g.ccu, g.languages, g.required_age, \
    g.is_free, g.detailed_description, g.dlc_steam_app_id, g.pc_requirements_minimum, g.metacritic_score, g.total_nr_recommendations, g.release_date, g.tag1, g.tag2, g.tag3, g.has_dlc \
    INTO games  \
    FROM games_pre g \
    UNION \
    SELECT s3.title, s3.genre1, s3.genre2,s3.genre3, s3.developer1, s3.developer2, s3.developer3, s3.publisher1, s3.publisher2, s3.publisher3, s3.total_ratings, s3.positivity, s3.boxleiter_number, \
    s3.revenue_gamestats, s3.revenue_boxleiter, s3.action, s3.adventure, s3.action_and_adventure, s3.has_another_developer, s3. has_publisher, s3.steam_app_id, Null as owners_estimation_steamspy, Null as owners_estimation_gameachievements, \
    Null as revenue_gameachievements, Null as average_playtime_forever, Null as average_playtime_2weeks, Null as median_playtime_forever, Null as median_playtime_2weeks, Null as current_price, \
    Null as initial_price, Null as discount, Null as ccu, Null as languages, Null as required_age, Null as is_free, Null as detailed_description, Null as dlc_steam_app_id, Null as pc_requirements_minimum, \
    Null as metacritic_score, NULL as total_nr_recommendations, NULL as release_date, NULL as tag1, NULL as tag2, NULL as tag3, NULL as has_dlc \
    FROM steamstore s3"

    table_altering_query = "ALTER TABLE games ADD temporary_id SERIAL"
    table_delete_duplicate_query = "DELETE FROM games \
    WHERE temporary_id NOT IN \
    (SELECT MAX(temporary_id) \
    FROM games \
    GROUP BY steam_app_id)"

    table_delete_duplicate_query2 = "DELETE FROM games \
    WHERE temporary_id NOT IN \
    (SELECT MAX(temporary_id) \
    FROM games \
    GROUP BY title)"

    table_altering_query2 = "ALTER TABLE games DROP temporary_id"

    table_altering_query3 = "ALTER TABLE games ADD CONSTRAINT steam_app_id_constraint PRIMARY KEY(steam_app_id)"
    table_altering_query4 = "ALTER TABLE games ADD CONSTRAINT title_constraint UNIQUE (title)"

    table_altering_query5 = "ALTER TABLE games ADD COLUMN Developed_Games_ID BIGINT"

    table_delete_query3 = "DROP TABLE IF EXISTS games_pre"

    table_altering_query6 = "ALTER TABLE games ADD COLUMN has_same_genre_id BIGINT"

    table_updating_query1 = """
    UPDATE games g
    SET  has_same_genre_id = (
    SELECT hsg.has_same_genre_id
    FROM has_same_genre hsg
    WHERE g.title = hsg.game_title
    LIMIT 1);"""

    table_altering_query7 = "ALTER TABLE games ADD CONSTRAINT FK_has_same_genre_id FOREIGN KEY (has_same_genre_id) REFERENCES has_same_genre (has_same_genre_id)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query2)
        cursor.execute(table_creation_query2)
        print("Altering Table")
        cursor.execute(table_altering_query)
        print("Deleting Duplicates")
        cursor.execute(table_delete_duplicate_query)
        cursor.execute(table_delete_duplicate_query2)
        cursor.execute(table_altering_query2)
        cursor.execute(table_altering_query3)
        cursor.execute(table_altering_query4)
        cursor.execute(table_altering_query5)
        cursor.execute(table_delete_query3)
        cursor.execute(table_altering_query6)
        cursor.execute(table_updating_query1)
        cursor.execute(table_altering_query7)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("execute_values() done")
    cursor.close()
    conn.close()


def update_relations():
    print(30 * "*")
    print("Update Games Relation to clean the data")

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    # I had problems loading null values from pandas to RDS. Therefore I update the 'NaN' now manually to null.
    updatequery_1 = """ UPDATE games SET developer1 = null WHERE developer1 ='NaN' """
    updatequery_2 = """ UPDATE games SET developer2 = null  WHERE developer2 ='NaN'"""
    updatequery_3 = """ UPDATE games SET developer3 = null  WHERE developer3 ='NaN'; """
    updatequery_4 = """ UPDATE games SET publisher1 = null  WHERE publisher1 ='NaN';"""
    updatequery_5 = """ UPDATE games SET publisher2 = null WHERE publisher2 ='NaN'; """
    updatequery_6 = """ UPDATE games SET publisher3 = null WHERE publisher3 ='NaN';"""
    updatequery_7 = """ UPDATE games SET total_ratings  = null WHERE total_ratings ='NaN';"""
    updatequery_8 = """ UPDATE games SET positivity  = null WHERE positivity ='NaN';"""
    updatequery_9 = """ UPDATE games SET revenue_gamestats  = null WHERE revenue_gamestats ='NaN'; """
    updatequery_10 = """ UPDATE games SET revenue_gamestats  = null WHERE revenue_gamestats ='NaN'; """
    updatequery_11 = """ UPDATE games SET revenue_boxleiter  = null WHERE revenue_boxleiter ='NaN'; """
    updatequery_12 = """ UPDATE games SET dlc_steam_app_id  = null WHERE dlc_steam_app_id  ='NaN';"""
    updatequery_13 = """ UPDATE games SET pc_requirements_minimum  = null WHERE pc_requirements_minimum  ='NaN';"""
    updatequery_14 = """ UPDATE games SET metacritic_score  = null WHERE metacritic_score  ='NaN'; """
    updatequery_15 = """ UPDATE games SET total_nr_recommendations  = null WHERE total_nr_recommendations  ='NaN';"""
    updatequery_16 = """ UPDATE games SET tag1  = null WHERE tag1 ='NaN'; """
    updatequery_17 = """ UPDATE games SET tag2  = null WHERE tag2 ='NaN';"""
    updatequery_18 = """ UPDATE games SET tag3  = null WHERE tag3 ='NaN';"""
    updatequery_19 = """ UPDATE games SET total_ratings = total_nr_recommendations  WHERE total_ratings  is null;"""
    updatequery_20 = """UPDATE games SET revenue_boxleiter  =  initial_price * total_ratings * boxleiter_number; """
    updatequery_21 = """UPDATE developers SET established_since  = null  WHERE established_since ='NaN'; """
    updatequery_22 = """UPDATE developers SET notes  = null  WHERE notes ='NaN'; """
    updatequery_23 = """UPDATE games SET revenue_gameachievements  =  initial_price * owners_estimation_gameachievements; """

    try:
        # Execute commands
        cursor.execute(updatequery_1)
        cursor.execute(updatequery_2)
        cursor.execute(updatequery_3)
        cursor.execute(updatequery_4)
        cursor.execute(updatequery_5)
        cursor.execute(updatequery_6)
        cursor.execute(updatequery_7)
        cursor.execute(updatequery_8)
        cursor.execute(updatequery_9)
        cursor.execute(updatequery_10)
        cursor.execute(updatequery_11)
        cursor.execute(updatequery_12)
        cursor.execute(updatequery_13)
        cursor.execute(updatequery_14)
        cursor.execute(updatequery_15)
        cursor.execute(updatequery_16)
        cursor.execute(updatequery_17)
        cursor.execute(updatequery_18)
        cursor.execute(updatequery_19)
        cursor.execute(updatequery_20)
        cursor.execute(updatequery_21)
        cursor.execute(updatequery_22)
        cursor.execute(updatequery_23)

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("execute_values() done")
    cursor.close()
    conn.close()


def create_developed_games_relation():
    print(30 * "*")
    print("Create Developed_Games Relation to normalize the data model")

    # ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    # USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    # PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    # DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE2"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE2"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE2"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE2"]

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")

    cursor = conn.cursor()

    table_delete_query = "DROP TABLE IF EXISTS Developed_Games CASCADE"

    table_creation_query = "CREATE TABLE IF NOT EXISTS Developed_Games ( \
    Developed_Games_ID SERIAL Primary Key, \
    Title VARCHAR(200) NOT NULL,  \
    Steam_App_ID BIGINT NOT NULL, \
    Developer_Name Varchar(200) NOT NULL, \
    Developer_ID BIGINT NOT NULL, \
    CONSTRAINT FK_Developer_Dev_Name FOREIGN KEY(Developer_Name) REFERENCES developers (Developer_Name), \
    CONSTRAINT FK_Developer_Dev_ID FOREIGN KEY(Developer_ID) REFERENCES developers(Developer_ID), \
    CONSTRAINT FK_Games_Title FOREIGN KEY(Title) REFERENCES games(title), \
    CONSTRAINT FK_Games_Steam_App_ID FOREIGN KEY(Steam_App_ID) REFERENCES games(Steam_App_ID))"

    table_enrichment_query1 = "INSERT INTO developers \
    SELECT DISTINCT developer1 FROM games  \
    LEFT OUTER JOIN developers ON developer_name = developer1 \
    WHERE developer_name IS NULL AND developer1 != 'NaN'  AND developer1 != ''; "

    table_enrichment_query2 = " INSERT INTO developers \
    SELECT DISTINCT developer2 FROM games \
    LEFT OUTER JOIN developers ON developer_name = developer2 \
    WHERE developer_name IS NULL AND developer2 != 'NaN' AND developer2 != ''; "

    table_enrichment_query3 = "INSERT INTO developers \
    SELECT DISTINCT developer3 FROM games \
    LEFT OUTER JOIN developers ON developer_name = developer3 \
    WHERE developer_name IS NULL AND developer3 != 'NaN' AND developer3 != '' ;"

    table_insert_query = "INSERT INTO Developed_Games (Title, Steam_App_ID, Developer_Name, Developer_ID) \
    SELECT \
    g.Title, \
    g.steam_app_id,\
    d.developer_name,  \
    d.developer_id \
    FROM Games g \
    INNER JOIN developers d on (d.developer_name = g.developer1) OR (d.developer_name = g.developer2) OR (d.developer_name = g.developer3);" \
 \
            table_updating_query1 = "UPDATE games g \
        SET developed_games_id  = ( \
        SELECT developed_games_id \
        FROM developed_games d \
        WHERE d.steam_app_id = g.steam_app_id \
        LIMIT 1)"

    table_updating_query2 = "UPDATE developers d \
        SET developed_games_id  = ( \
        SELECT developed_games_id \
        FROM developed_games dg \
        WHERE d.developer_id  = dg.developer_id  \
        LIMIT 1)"

    table_altering_query = "ALTER TABLE games ADD CONSTRAINT FK_Developed_Games FOREIGN KEY (Developed_Games_ID) REFERENCES Developed_Games (Developed_Games_ID)"
    table_altering_query2 = "ALTER TABLE developers ADD CONSTRAINT FK_Developed_Games FOREIGN KEY (Developed_Games_ID) REFERENCES Developed_Games (Developed_Games_ID)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        print("Enrichment of Data in the Developer Table")
        cursor.execute(table_enrichment_query1)
        cursor.execute(table_enrichment_query2)
        cursor.execute(table_enrichment_query3)
        print("Inserting and Updating Data into Table")
        cursor.execute(table_insert_query)
        cursor.execute(table_updating_query1)
        cursor.execute(table_updating_query2)
        print("Altering old tables")
        cursor.execute(table_altering_query)
        cursor.execute(table_altering_query2)

        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("execute_values() done")
        print(error)

    cursor.close()
    conn.close()


def lambda_handler(event, context):
    # read data
    developer, game_achievements, steamstore, steam, steamspy = read_data_from_s3()

    # clean/transform data in pandas
    developer_cleaned = clean_developer(developer)
    game_achievements_cleaned = clean_game_achievements(game_achievements)
    steamstore_cleaned = clean_steamstore(steamstore)
    steam_cleaned = clean_steam(steam)
    steamspy_cleaned = clean_steamspy(steamspy)

    # load data to rds and table creation
    load_developer_to_postgresql(developer_cleaned)
    load_game_achievements_to_postgresql(game_achievements_cleaned)
    load_steamstore_to_postgresql(steamstore_cleaned)
    load_steam_to_postgresql(steam_cleaned)
    load_steamspy_to_postgresql(steamspy_cleaned)
    update_relations()
    create_developed_games_relation()