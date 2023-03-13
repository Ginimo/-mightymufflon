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

s3 = boto3.client('s3')


def lambda_handler(event, context):
    bucket = os.environ["ENV_BUCKET_NAME"]

    # Defining global api-key
    api_key = os.environ["ENV_API_KEY"]

    # Creating empty lists for transfer to s3-bucket
    movies = []
    series = []

    # Extract top-rated movies
    for page_num in range(1, 501):
        url_top = "https://api.themoviedb.org/3/movie/top_rated?api_key=" + api_key + "&language=en-US&page=" + str(
            page_num)
        r_top = requests.get(url_top)
        data_movies = r_top.json()
        data_movies = pd.json_normalize(data_movies["results"])
        movies.append(data_movies)

    # Extract total pages for series
    url_series = "https://api.themoviedb.org/3/tv/top_rated?api_key=" + api_key + "&language=en-US&page=1"
    r_series = requests.get(url_series)
    data_series = r_series.json()
    series_pages = data_series["total_pages"]

    # Extract series
    for page_num in range(1, 121):
        url_series = "https://api.themoviedb.org/3/tv/top_rated?api_key=" + api_key + "&language=en-US&page=" + str(
            page_num)
        r_series = requests.get(url_series)
        data_series_all = r_series.json()
        data_series_all = pd.json_normalize(data_series_all["results"])
        series.append(data_series_all)

    # Extract movie genres
    url_movie_genres = "https://api.themoviedb.org/3/genre/movie/list?api_key=" + api_key + "&language=en-US"
    r_movie_genres = requests.get(url_movie_genres)
    data_movie_genres = r_movie_genres.json()
    movie_genres = pd.json_normalize(data_movie_genres["genres"])

    # Extract series genres
    url_series_genres = "https://api.themoviedb.org/3/genre/tv/list?api_key=" + api_key + "&language=en-US"
    r_series_genres = requests.get(url_series_genres)
    data_series_genres = r_series_genres.json()
    series_genres = pd.json_normalize(data_series_genres["genres"])

    # Write json files to s3-bucket
    ############################################################################

    # Concate the movies and series list into a dataframe:
    movies = pd.concat(movies)
    series = pd.concat(series)

    csv_buffer1 = StringIO()
    csv_buffer2 = StringIO()
    csv_buffer3 = StringIO()
    csv_buffer4 = StringIO()

    # Doing the naming of the files:
    movies.to_csv(csv_buffer1)
    series.to_csv(csv_buffer2)
    movie_genres.to_csv(csv_buffer3)
    series_genres.to_csv(csv_buffer4)

    # Call the s3-Bucket --> Key = Filename / Body = Content
    s3_resource = boto3.resource("s3")

    s3_resource.Object(bucket, "movie.csv").put(Body=csv_buffer1.getvalue())
    s3_resource.Object(bucket, "series.csv").put(Body=csv_buffer2.getvalue())
    s3_resource.Object(bucket, "movie_genre.csv").put(Body=csv_buffer3.getvalue())
    s3_resource.Object(bucket, "series_genre.csv").put(Body=csv_buffer4.getvalue())

    print("Put Complete")