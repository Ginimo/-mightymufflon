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

s3_client = boto3.client('s3')


def read_data_from_s3():
    # Define bucket-name:
    bucket = os.environ["ENV_BUCKET_NAME"]

    # Define the csv to read from s3-Bucket:
    movie_data = "movie.csv"
    movie_genre_data = "movie_genre.csv"
    series_data = "series.csv"
    series_genre_data = "series_genre.csv"

    # Define the responses from s3:
    response_movie = s3_client.get_object(Bucket=bucket, Key=movie_data)
    response_movie_genres = s3_client.get_object(Bucket=bucket, Key=movie_genre_data)
    response_series = s3_client.get_object(Bucket=bucket, Key=series_data)
    response_series_genre = s3_client.get_object(Bucket=bucket, Key=series_genre_data)

    # Define status from s3:
    status_movie = response_movie.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_movie_genres = response_movie_genres.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_series = response_series.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_series_genre = response_series_genre.get("ResponseMetadata", {}).get("HTTPStatusCode")

    ############################################################################
    # Load movies
    if status_movie == 200:
        print(f"Successful S3 get_object response. Status - {status_movie}")
        df_movie = pd.read_csv(response_movie.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_movie}")

    ############################################################################
    # Load movies-genres
    if status_movie_genres == 200:
        print(f"Successful S3 get_object response. Status - {status_movie_genres}")
        df_movie_genre = pd.read_csv(response_movie_genres.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_movie}")

    ############################################################################
    # Load series
    if status_series == 200:
        print(f"Successful S3 get_object response. Status - {status_series}")
        df_series = pd.read_csv(response_series.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_movie}")

    ############################################################################
    if status_movie_genres == 200:
        print(f"Successful S3 get_object response. Status - {status_series_genre}")
        df_series_genre = pd.read_csv(response_series_genre.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status_series_genre}")

    # Return the loaded data-frames:
    return df_movie, df_movie_genre, df_series, df_series_genre


def clean_movie(movie, movie_genre):
    # Load data
    df_movie = movie
    df_movie_genre = movie_genre

    # Start cleaning and transformation
    df_movie_genre = df_movie_genre[["id", "name"]]
    df_movie = df_movie.iloc[:, 1:]
    df_movie["genre_ids"] = df_movie["genre_ids"].apply(eval)
    df_movie_expand = df_movie["genre_ids"].apply(pd.Series)
    df_movie_expand = df_movie_expand.iloc[:, 0:2]
    df_movie_expand = df_movie_expand.rename(columns={0: "genre_1", 1: "genre_2"})
    df_movie = df_movie.loc[:, df_movie.columns != 'genre_ids']
    df_movies = pd.concat([df_movie, df_movie_expand], axis="columns")
    df_movies = df_movies.dropna(subset=["genre_1"], inplace=False)
    df_movies["genre_1"] = df_movies["genre_1"].astype(int)

    df_check = pd.merge(df_movies, df_movie_genre,
                        left_on="genre_1",
                        right_on="id",
                        how="left")

    df_check = df_check.loc[:, df_check.columns != 'id_y']
    df_check = df_check.rename(columns={'name': 'genre_name1'})

    # Handling of missing values
    df_check["genre_2"] = df_check["genre_2"].fillna(0)
    df_check["genre_2"] = df_check["genre_2"].astype(int)

    # Merge the dataframes with the genre-name:
    df_check = pd.merge(df_check, df_movie_genre,
                        left_on="genre_2",
                        right_on="id",
                        how="left")

    df_check = df_check.loc[:, df_check.columns != 'id']
    df_check = df_check.rename(columns={'name': 'genre_name2'})
    df_check = df_check.rename(columns={'id_x': 'id'})
    df_check["genre_accumulated"] = df_check["genre_name1"].astype(str) + "," + df_check["genre_name2"].astype(str)

    # Remove the nan values for the second genre-name if value is na:
    genre_acc = []

    for i in df_check["genre_accumulated"]:
        x = i.split(",")
        if x[1] == "nan":
            genre_acc.append(x[0])
        else:
            genre_acc.append(str(x[0] + "," + x[1]))

            # Insert list as new column
    df_check["genre_accumulated2"] = genre_acc
    # Rename and adjust columns
    df_check = df_check.loc[:, df_check.columns != 'genre_accumulated']
    df_check = df_check.rename(columns={'genre_accumulated2': 'genre_accumulated'})

    # Make booleans for merge
    action_bool = []
    adventure_bool = []
    action_adventure_bool = []

    genre_check = df_check[["genre_name1", "genre_name2"]]

    for movie1, movie2 in genre_check.itertuples(index=False):
        if (movie1 == "Action" and movie2 != "Adventure") or (movie1 != "Adventure" and movie2 == "Action"):
            action_bool.append(True)
        else:
            action_bool.append(False)

    for movie1, movie2 in genre_check.itertuples(index=False):
        if (movie1 == "Adventure" and movie2 != "Action") or (movie1 != "Action" and movie2 == "Adventure"):
            adventure_bool.append(True)
        else:
            adventure_bool.append(False)

    for movie1, movie2 in genre_check.itertuples(index=False):
        if (movie1 == "Adventure" and movie2 == "Action") or (movie1 == "Action" and movie2 == "Adventure"):
            action_adventure_bool.append(True)
        else:
            action_adventure_bool.append(False)

    df_check["action"] = action_bool
    df_check["adventure"] = adventure_bool
    df_check["action_and_adventure"] = action_adventure_bool

    # Adjust release_date
    release_date = []
    release_year = []

    for date in df_check["release_date"]:
        date = date.replace("-", "")
        date = date[0:4] + date[4:6] + "01"
        release_date.append(date)

    for date in release_date:
        date = date[0:4]
        release_year.append(date)

    # Remove original release-date and append new release_date and release_year:
    df_check.drop(["release_date"], axis=1)
    df_check["release_date"] = release_date
    df_check["release_year"] = release_year

    return df_check


def clean_series(series, series_genre):
    # Load data
    df_series = series
    df_series_genre = series_genre

    # Start cleaning and transformation
    df_series_genre = df_series_genre[["id", "name"]]
    df_series = df_series.iloc[:, 1:]
    df_series["genre_ids"] = df_series["genre_ids"].apply(eval)
    df_series_expand = df_series["genre_ids"].apply(pd.Series)
    df_series_expand = df_series_expand.iloc[:, 0:2]
    df_series_expand = df_series_expand.rename(columns={0: "genre_1", 1: "genre_2"})
    df_series = df_series.loc[:, df_series.columns != 'genre_ids']
    df_series = pd.concat([df_series, df_series_expand], axis="columns")

    # Handling of missing values
    df_series = df_series.dropna(subset=["genre_1"], inplace=False)
    df_series["genre_1"] = df_series["genre_1"].astype(int)
    # Merge the dataframes with the genre-name:
    df_check_s = pd.merge(df_series, df_series_genre,
                          left_on="genre_1",
                          right_on="id",
                          how="left")

    df_check_s = df_check_s.loc[:, df_check_s.columns != 'id_y']
    df_check_s = df_check_s.rename(columns={'name_y': 'genre_name1'})
    df_check_s["genre_2"] = df_check_s["genre_2"].fillna(0)
    df_check_s["genre_2"] = df_check_s["genre_2"].astype(int)

    # Merge the dataframes with the genre-name:
    df_check_s = pd.merge(df_check_s, df_series_genre,
                          left_on="genre_2",
                          right_on="id",
                          how="left")

    df_check_s = df_check_s.loc[:, df_check_s.columns != 'id']
    df_check_s = df_check_s.rename(columns={'name': 'genre_name2'})
    df_check_s = df_check_s.rename(columns={'id_x': 'id'})
    df_check_s["genre_accumulated"] = df_check_s["genre_name1"].astype(str) + "," + df_check_s["genre_name2"].astype(
        str)

    # Remove the nan values for the second genre-name if value is na:
    genre_acc_s = []

    for i in df_check_s["genre_accumulated"]:
        x = i.split(",")
        if x[1] == "nan":
            genre_acc_s.append(x[0])
        else:
            genre_acc_s.append(str(x[0] + "," + x[1]))

            # Insert list as new column
    df_check_s["genre_accumulated2"] = genre_acc_s

    # Rename and adjust columns
    df_check_s = df_check_s.loc[:, df_check_s.columns != "genre_accumulated"]
    df_check_s = df_check_s.loc[:, df_check_s.columns != "origin_country"]
    df_check_s = df_check_s.rename(columns={"genre_accumulated2": "genre_accumulated",
                                            "original_name": "original_title",
                                            "name_x": "title",
                                            "first_air_date": "release_date"})

    # Make booleans for merge:
    action_adventure_bool = []

    genre_check = df_check_s[["genre_name1", "genre_name2"]]

    for serie1, serie2 in genre_check.itertuples(index=False):
        if (serie1 == "Action & Adventure" and serie2 != "Action & Adventure") or (
                serie1 != "Action & Adventure" and serie2 == "Action & Adventure"):
            action_adventure_bool.append(True)
        else:
            action_adventure_bool.append(False)

    df_check_s["action_and_adventure"] = action_adventure_bool
    df_check_s = df_check_s.fillna({"action_and_adventure": False})

    # Adjust release_date
    df_check_s["release_date"] = df_check_s["release_date"].astype(str)
    release_date = []
    release_year = []

    for date in df_check_s["release_date"]:
        date = date.replace("-", "")
        date = date[0:4] + date[4:6] + "01"
        release_date.append(date)

    for date in release_date:
        date = date[0:4]
        release_year.append(date)

    # Remove original release-date and append new release_date and release_year:
    df_check_s.drop(["release_date"], axis=1)
    df_check_s["release_date"] = release_date
    df_check_s["release_year"] = release_year

    # Remove the NAN-Values in the release-date of the series.
    df_check_s = df_check_s.loc[df_check_s["release_date"] != "nan01"]

    return df_check_s


def load_movies_postgresql(movie):
    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    df = movie

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

    table_delete_query = "DROP TABLE IF EXISTS movies"
    table_creation_query = "CREATE TABLE IF NOT EXISTS movies (adult VARCHAR(5), backdrop_path VARCHAR(500), id real, original_language VARCHAR(2), original_title VARCHAR(100), overview VARCHAR(1000), popularity real, poster_path VARCHAR(100), release_date DATE, title VARCHAR(100), video VARCHAR(5), vote_average real, vote_count real, genre_1 real, genre_2 real, genre_name1 VARCHAR(50), genre_name2 VARCHAR(50), genre_accumulated VARCHAR(100), action BOOLEAN, adventure BOOLEAN, action_and_adventure BOOLEAN, release_year VARCHAR(4))"

    try:
        print("Creating Table")
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "movies"

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


def load_series_postgresql(series):
    ENDPOINT = os.environ["ENV_ENDPOINT_POSTGRE"]
    USERNAME = os.environ["ENV_USERNAME_POSTGRE"]
    PASSWORD = os.environ["ENV_PASSWORD_POSTGRE"]
    DB_NAME = os.environ["ENV_DB_NAME_POSTGRE"]

    df = series

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

    table_delete_query = "DROP TABLE IF EXISTS series"
    table_creation_query = "CREATE TABLE IF NOT EXISTS series (backdrop_path VARCHAR(500), release_date DATE, id real, title VARCHAR(100), original_language VARCHAR(2), original_title VARCHAR(100), overview VARCHAR(5000), popularity real, poster_path VARCHAR(100), vote_average real, vote_count real, genre_1 real, genre_2 real, genre_name1 VARCHAR(50), genre_name2 VARCHAR(50), genre_accumulated VARCHAR(100), action_and_adventure BOOLEAN, release_year VARCHAR(4))"
    # alter_series_query = "ALTER TABLE series ALTER release_date type date using(release_date::date)"

    try:
        print("Creating Table")
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "series"

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

    # try:
    # print("Change Series release_date column")
    # cursor.execute(alter_series_query)
    # conn.commit()
    # except (Exception, psycopg2.DatabaseError) as error:
    # print(error)

    cursor.close()
    conn.close()


def create_relationship_table():
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

    table_delete_query1 = "DROP TABLE IF EXISTS has_same_genre_check"

    table_creation_query1 = "CREATE TABLE IF NOT EXISTS has_same_genre_check (\
                            game_title VARCHAR(200),\
                            movie_and_series_vote_average real,\
                            movie_and_series_vote_count real,\
                            movie_and_series_popularity real)"

    table_insert_query1 = "INSERT INTO has_same_genre_check (game_title, movie_and_series_vote_average, movie_and_series_vote_count, movie_and_series_popularity) \
                        SELECT g.title AS game_title, AVG(m.vote_average) AS movie_and_series_vote_average,\
                        SUM(m.vote_count) AS movie_and_series_vote_count, SUM(m.popularity) AS movie_and_series_popularity \
                        FROM games AS g \
                        LEFT JOIN movies AS m ON(g.action = m.action) AND (g.release_date = m.release_date) \
                        WHERE g.action = true \
                        GROUP BY g.title \
                        UNION \
                        SELECT g.title AS game_title, AVG(m.vote_average) AS movie_and_series_vote_average, \
                        SUM(m.vote_count) AS movie_and_series_vote_count, SUM(m.popularity) AS movie_and_series_popularity \
                        FROM games AS g \
                        LEFT JOIN movies AS m ON(g.adventure = m.adventure) AND (g.release_date = m.release_date) \
                        WHERE g.adventure = true \
                        GROUP BY g.title \
                        UNION \
                        SELECT g.title AS game_title, AVG(m.vote_average) AS movie_and_series_vote_average, \
                        SUM(m.vote_count) AS movie_and_series_vote_count, SUM(m.popularity) AS movie_and_series_popularity \
                        FROM games AS g \
                        LEFT JOIN movies AS m ON(g.action_and_adventure = m.action_and_adventure) AND (g.release_date = m.release_date) \
                        WHERE g.action_and_adventure = true \
                        GROUP BY g.title \
                        UNION \
                        SELECT g.title AS game_title, avg(s.vote_average) AS movie_and_series_vote_average, \
                        SUM(s.vote_count) AS movie_and_series_vote_count, SUM(s.popularity) AS movie_and_series_popularity \
                        FROM games AS g \
                        LEFT JOIN series AS s ON(g.action_and_adventure = s.action_and_adventure) AND (g.release_date = s.release_date) \
                        WHERE g.action_and_adventure = true \
                        GROUP BY g.title"

    table_delete_query2 = "DROP TABLE IF EXISTS has_same_genre CASCADE"

    table_creation_query2 = "CREATE TABLE IF NOT EXISTS has_same_genre (\
                            Has_Same_Genre_ID SERIAL Primary Key, \
                            game_title VARCHAR(200),\
                            movie_and_series_vote_average real,\
                            movie_and_series_vote_count real,\
                            movie_and_series_popularity real, \
                            has_successful_movie_or_serie boolean, \
                            CONSTRAINT FK_Game_Title FOREIGN KEY(game_title) REFERENCES games (title))"

    table_insert_query2 = "INSERT INTO has_same_genre (game_title, movie_and_series_vote_average, movie_and_series_vote_count, movie_and_series_popularity, has_successful_movie_or_serie) \
                            SELECT *, \
                            CASE WHEN movie_and_series_vote_average > 6.55 THEN True \
                            ELSE False \
                            END AS has_successful_movie_or_serie \
                            FROM has_same_genre_check"

    table_delete_query3 = "DROP TABLE IF EXISTS has_same_genre_check"

    try:
        print("Creating Relationship Table")
        cursor.execute(table_delete_query1)
        cursor.execute(table_creation_query1)
        cursor.execute(table_insert_query1)
        cursor.execute(table_delete_query2)
        cursor.execute(table_creation_query2)
        cursor.execute(table_insert_query2)
        cursor.execute(table_delete_query3)
        print("Values successfully inserted in Relationship Table")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    cursor.close()
    conn.close()


def lambda_handler(event, context):
    mov, mov_genre, ser, ser_genre = read_data_from_s3()
    movie_clean = clean_movie(mov, mov_genre)
    series_clean = clean_series(ser, ser_genre)
    load_movies_postgresql(movie_clean)
    load_series_postgresql(series_clean)
    create_relationship_table()