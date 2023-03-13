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
import sys
import botocore
from pprint import pprint
import psycopg2
import psycopg2.extras as extras

#Test

#Joel erklärt die Welt

s3 = boto3.client("s3",
                  region_name="us-east-1",
                  aws_access_key_id="ASIAQQFB54XAO2QMBYA3",
                  aws_secret_access_key="7jRV2Pb8yiHp5F9z6n3YS4Thhz+OXhMPOK0F4EAW",
                  aws_session_token="FwoGZXIvYXdzEB0aDEN7Gxn8UiOkuC/abCK4AV8jE5rGcbDfA1fT1RTt8ftxAIBYi"
                                    "zE5rh7uoau9BTmx1oGSuHshFHeIweGUioLChFUT970HGcc4A3IhUXM1zB5RtmsEiOg"
                                    "PULry5I9Woqw+2IS+XLVhA9/wu1Z7h72hLGX3tEfbADc3RXg3FHSwNpOfxtpaFySlz"
                                    "KdEY4lTaTKwHw6WXLPytkq8zM1HIwI+tTHQKMG65myZXT4TTi5+irlKHf0eYXDeibT5"
                                    "vr9g/nkBnBZ+bPa0W1Mot9nDnAYyLV6TJ1lhWlmbXEkcFUU91MlK+QWoJwaufzzP1Hr"
                                    "F8jDXA0WGgU84EDdQiu09KA=="
                                    )


def read_data_from_s3():
    # to read files from the s3 bucket.
    # file_path: refers to the s3 bucket"s path for read data.
    response = s3.get_object(Bucket="gamelake", Key="master/master.csv")
    df_twitch = pd.read_csv(response.get("Body"), )
    # Return the loaded data-frames:
    return df_twitch


def cleaned_twitch(df_twitch):
    # Check for Duplicates
    print("Check for duplicates:")
    duplicates = df_twitch[df_twitch.duplicated(keep=False)]
    pprint(duplicates)
    df_twitch_cleaned = df_twitch.drop(
        columns=["Unnamed: 0", "type", "thumbnail_url", "tag_ids"])  # normalization of the data
    print(list(df_twitch_cleaned.columns))
    print("Check for Missing Values")
    print("The following columns have missing values",
          df_twitch_cleaned.columns[df_twitch_cleaned.isna().any(0)].tolist())
    return df_twitch_cleaned


def load_twitch_to_postgresql(df_twitch_cleaned):
    print(30 * "*")
    print("Loading Twitch")

    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]
    df = df_twitch_cleaned
    conn = None
    try:
        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    cursor = conn.cursor()
    table_delete_query = "DROP TABLE IF EXISTS twitch"
    table_creation_query = "CREATE TABLE IF NOT EXISTS twitch ( id BIGINT not null, user_id BIGINT, user_login VARCHAR(500), user_name VARCHAR(500), game_id BIGINT, game_name VARCHAR(500), title VARCHAR(500), viewer_count BIGINT, started_at VARCHAR(30), language VARCHAR(10), is_mature VARCHAR(10))"
    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    table = "twitch"
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
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


def create_streamed_games():
    print(30 * "*")
    print("Create Streamed_Games Relation to normalize the data model")
    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]
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
    table_delete_query = "DROP TABLE IF EXISTS Streamed_Games CASCADE"
    table_creation_query = "CREATE TABLE IF NOT EXISTS Streamed_Games(" \
                           "Streamed_Games_ID SERIAL Primary Key, " \
                           "user_name VARCHAR(500)," \
                           "title VARCHAR(500)," \
                           "language VARCHAR(10)," \
                           "viewer_count BIGINT," \
                           "Steam_App_ID BIGINT NOT NULL)"

    table_insert_query = "INSERT INTO Streamed_Games (user_name, title, language, viewer_count, steam_app_id) \
        SELECT \
        t.user_name, \
        t.title,\
        t.language,  \
        t.viewer_count,\
        g.steam_app_id \
        FROM twitch t \
        INNER JOIN games g on t.title = g.title"

    table_altering_query = "ALTER TABLE Streamed_Games ADD CONSTRAINT fk_streams_games FOREIGN KEY(steam_app_id) REFERENCES games (steam_app_id)"

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        print("Enrichment of Data in the Developer Table")
        print("Inserting and Updating Data into Table")
        cursor.execute(table_insert_query)
        # cursor.execute(table_updating_query)
        cursor.execute(table_altering_query)

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("execute_values() done")
        print(error)
    cursor.close()
    conn.close()


def lambda_handler(event, context):
    # read data
    df_twitch = read_data_from_s3()
    # clean/transform data in pandas
    df_twitch_cleaned = cleaned_twitch(df_twitch)
    # load data to rds and table creation
    load_twitch_to_postgresql(df_twitch_cleaned)
    #create new table streamed_games
    create_streamed_games()


