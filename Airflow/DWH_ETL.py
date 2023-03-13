import json
import pandas as pd
import numpy as np
import psycopg2
import boto3
from botocore.exceptions import ClientError
import psycopg2.extras as extras
import sys
from pprint import pprint

s3_client = boto3.client('s3')


## Connection
def open_connection(ENDPOINT, USERNAME, PASSWORD, DB_NAME):
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor, conn


def close_connection(conn):
    conn.close()
    print("Connection closed")


# Get Data

def get_data_from_dl_cleaned_zone(cursor, conn):
    select_query = """
    SELECT g.title,
        release_date,
        d.country,
        d.city,
        genre1,
        genre2,
        genre3,
        tag1,
        tag2,
        tag3,
        is_free,
        required_age,
        has_publisher,
        has_another_developer,
        d.developer_name,
        d.developer_type,
        has_dlc,
        initial_price,
        discount,
        revenue_gameachievements,
        revenue_gamestats,
        revenue_boxleiter,
        total_ratings as total_ratings_steam,
        positivity as positivity_steam,
        average_playtime_forever,
        median_playtime_forever,
       sg.user_name,
       sg."language", 
       sg.viewer_count,
       hsg.has_successful_movie_or_serie
    FROM games g
    LEFT JOIN developed_games dg on dg.steam_app_id = g.steam_app_id
    LEFT JOIN developers d on d.developed_games_id = dg.developed_games_id
    LEFT JOIN streamed_games sg  on g.steam_app_id = sg.steam_app_id
    LEFT JOIN has_same_genre hsg on hsg.has_same_genre_id  = g.has_same_genre_id
    ORDER BY g.title;
      """
    df = pd.read_sql_query(select_query, conn)

    # Validation of data
    print("Validation of the data:")
    select_query_validation = """
    SELECT *
    FROM games
    ORDER BY title
    LIMIT 2;
    """
    cursor.execute(select_query_validation)
    for row in cursor:
        pprint(dict(row))

    return df


# Load Data

def load_data_to_dwh(cursor, conn, df):
    table_delete_query = "DROP TABLE IF EXISTS fact_sheet CASCADE"

    table_creation_query = """
    CREATE TABLE IF NOT EXISTS fact_sheet
    (title VARCHAR(100),
    release_date DATE,
    country VARCHAR (50),
    city VARCHAR (50),
    genre1 VARCHAR (50),
    genre2 VARCHAR (50),
    genre3 VARCHAR (50),
    tag1 VARCHAR (50),
    tag2 VARCHAR (50),
    tag3 VARCHAR (50),
    is_free BOOLEAN,
    required_age REAL,
    has_publisher BOOLEAN,
    has_another_developer BOOLEAN,
    developer_name VARCHAR(100),
    developer_type VARCHAR(20),
    has_dlc BOOLEAN,
    initial_price REAL,
    discount REAL,
    revenue_gameachievements REAL,
    revenue_gamestats REAL,
    revenue_boxleiter REAL,
    total_ratings_steam REAL,
    positivity_steam REAL,
    average_playtime_forever REAL,
    median_playtime_forever REAL,
    user_name VARCHAR(50),
    language char(10),
    viewer_count REAL,
    has_successful_movie_or_serie BOOLEAN);
    """

    try:
        print("Creating Table")
        # Execute commands
        cursor.execute(table_delete_query)
        cursor.execute(table_creation_query)
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    table = "fact_sheet"

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    insert_query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    try:
        extras.execute_values(cursor, insert_query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")

    # I had problems loading null values from pandas to RDS. Therefore I update the 'NaN' now manually to null.
    updatequery_1 = """ UPDATE fact_sheet SET total_ratings_steam  = null WHERE total_ratings_steam ='NaN';"""
    updatequery_2 = """ UPDATE fact_sheet SET positivity_steam  = null WHERE positivity_steam ='NaN';"""
    updatequery_3 = """ UPDATE fact_sheet SET genre1  = null WHERE genre1 ='NaN'; """
    updatequery_4 = """ UPDATE fact_sheet SET genre2  = null WHERE genre2 ='NaN';"""
    updatequery_5 = """ UPDATE fact_sheet SET genre3  = null WHERE genre3 ='NaN';"""
    updatequery_6 = """ UPDATE fact_sheet SET required_age  = null WHERE required_age ='NaN';"""
    updatequery_7 = """ UPDATE fact_sheet SET revenue_gamestats  = null WHERE revenue_gamestats ='NaN'; """
    updatequery_8 = """ UPDATE fact_sheet SET revenue_gameachievements  = null WHERE revenue_gameachievements ='NaN'; """
    updatequery_9 = """ UPDATE fact_sheet SET revenue_boxleiter  = null WHERE revenue_boxleiter ='NaN'; """
    updatequery_10 = """ UPDATE fact_sheet SET tag1  = null WHERE tag1 ='NaN'; """
    updatequery_11 = """ UPDATE fact_sheet SET tag2  = null WHERE tag2 ='NaN';"""
    updatequery_12 = """ UPDATE fact_sheet SET tag3  = null WHERE tag3 ='NaN';"""
    updatequery_13 = """ UPDATE fact_sheet SET initial_price  = null WHERE initial_price ='NaN';"""
    updatequery_14 = """ UPDATE fact_sheet SET discount  = null WHERE discount ='NaN';"""
    updatequery_15 = """ UPDATE fact_sheet SET average_playtime_forever  = null WHERE average_playtime_forever ='NaN';"""
    updatequery_16 = """ UPDATE fact_sheet SET median_playtime_forever  = null WHERE median_playtime_forever ='NaN';"""
    updatequery_17 = """ UPDATE fact_sheet SET user_name = null WHERE user_name ='NaN'; """
    updatequery_18 = """ UPDATE fact_sheet SET "language" = null WHERE "language" ='NaN';  """
    updatequery_19 = """ UPDATE fact_sheet SET viewer_count = null WHERE viewer_count ='NaN'; """
    # updatequery_20 = """ UPDATE fact_sheet SET has_successful_movie_or_serie = null WHERE has_successful_movie_or_serie = 'NaN'; """

    delete_view1 = """ DROP VIEW IF EXISTS Consultant_Top_Title """
    delete_view2 = """ DROP VIEW IF EXISTS Consultant_Line_Chart_Boxleiter """
    delete_view3 = """ DROP VIEW IF EXISTS Consultant_Line_Chart_Game_Achievements """
    delete_view4 = """ DROP VIEW IF EXISTS Consultant_Title """
    delete_view5 = """ DROP VIEW IF EXISTS Consultant_General_KPI """
    delete_view6 = """ DROP VIEW IF EXISTS game_artist_genres_by_release """
    delete_view7 = """ DROP VIEW IF EXISTS game_artist_revenue_by_genre """
    delete_view8 = """ DROP VIEW IF EXISTS game_artist_revenue_by_tag """
    delete_view9 = """ DROP VIEW IF EXISTS """
    delete_view10 = """ DROP VIEW IF EXISTS game_artist_top_indiedeveloper_revenue """
    delete_view11 = """ DROP VIEW IF EXISTS game_artist_top_indiedeveloper_rating """
    delete_view12 = """ DROP VIEW IF EXISTS game_artist_revenue_country_genre """
    delete_view13 = """ DROP VIEW IF EXISTS sm_developer_country """
    delete_view14 = """ DROP VIEW IF EXISTS sm_top_viewed_games """

    create_view1 = """ CREATE OR REPLACE VIEW Consultant_Top_Title AS
SELECT title,
AVG(revenue_gameachievements) as game_achievments_revenue,
AVG(revenue_gamestats) as gamestats_revenue,
AVG(revenue_boxleiter) as boxleiter_revenue,
AVG(average_playtime_forever) as playtime,
AVG(positivity_steam) as positivity_steam,
AVG(viewer_count) as viewer_count
FROM fact_sheet
GROUP BY title
HAVING AVG(revenue_boxleiter) is not null;
  """

    create_view2 = """  CREATE OR REPLACE VIEW Consultant_Line_Chart_Boxleiter AS
    SELECT title,
release_date,
developer_type,
AVG(revenue_boxleiter) as boxleiter_revenue,
AVG(average_playtime_forever) as playtime
FROM fact_sheet
GROUP BY title, release_date, developer_type
HAVING AVG(revenue_boxleiter)  is not null AND  AVG(average_playtime_forever) is not null ;
  """

    create_view3 = """ CREATE OR REPLACE VIEW Consultant_Line_Chart_Game_Achievements AS
SELECT title,
release_date,
developer_type,
AVG(revenue_gameachievements) as game_achievements_revenue,
AVG(average_playtime_forever) as playtime
FROM public.fact_sheet
GROUP BY title, release_date, developer_type
HAVING AVG(revenue_gameachievements)  is not null AND  AVG(average_playtime_forever) is not null;  """

    create_view4 = """CREATE OR REPLACE VIEW Consultant_Title AS
SELECT title
FROM fact_sheet
GROUP BY title;"""

    create_view5 = """ CREATE OR REPLACE VIEW Consultant_General_KPI AS
SELECT title, is_free, developer_type,revenue_boxleiter
FROM fact_sheet
GROUP BY title,is_free, developer_type,revenue_boxleiter ; """

    create_view6 = """create or replace view game_artist_genres_by_release as
select distinct title, release_date, genre1, genre2
from public.fact_sheet
where release_date > '2000-01-01';"""

    create_view7 = """create or replace view game_artist_revenue_by_genre as
select distinct title, genre1, genre2, revenue_gameachievements, revenue_gamestats, revenue_boxleiter 
from public.fact_sheet
where genre1 <> 'NULL';"""

    create_view8 = """create or replace view game_artist_revenue_by_tag as
select distinct title, genre1, genre2, tag1, tag2, tag3, total_ratings_steam, revenue_gameachievements, revenue_gamestats, revenue_boxleiter
from public.fact_sheet
where tag1 <> 'NULL';"""

    create_view10 = """create or replace view game_artist_top_indiedeveloper_revenue as
select distinct developer_name, sum(revenue_boxleiter)
from fact_sheet
where revenue_boxleiter is not null and developer_name is not null and developer_type = 'indie'
group by developer_name
order by sum(revenue_boxleiter) desc
limit 5;"""

    create_view11 = """create or replace view game_artist_top_indiedeveloper_rating as
select distinct developer_name, sum(total_ratings_steam)
from fact_sheet
where total_ratings_steam is not null and developer_name is not null and developer_type = 'indie'
group by developer_name
order by sum(total_ratings_steam) desc
limit 5;"""

    create_view12 = """create or replace view game_artist_revenue_country_genre as
select country , genre1, sum(revenue_boxleiter) as boxleiter
from fact_sheet
where (country is not null and genre1 is not null)
group by country, genre1
order by boxleiter DESC NULLS LAST;"""

    create_view13 = """CREATE OR REPLACE VIEW public.sm_developer_country AS
   SELECT fact_sheet.developer_name,
    fact_sheet.title,
    fact_sheet.city,
    fact_sheet.country,
    fact_sheet.release_date,
    fact_sheet.revenue_boxleiter,
    fact_sheet.genre1
   FROM fact_sheet
  WHERE fact_sheet.city IS NOT NULL AND fact_sheet.country IS NOT NULL AND fact_sheet.release_date IS NOT NULL AND fact_sheet.revenue_boxleiter IS NOT NULL AND fact_sheet.genre1 IS NOT NULL;"""

    create_view14 = """CREATE OR REPLACE VIEW public.sm_top_viewed_games AS
   SELECT fact_sheet.title,
    fact_sheet.user_name,
    sum(fact_sheet.viewer_count) AS sum,
    fact_sheet.language
   FROM fact_sheet
  WHERE fact_sheet.viewer_count IS NOT NULL
  GROUP BY fact_sheet.title, fact_sheet.user_name, fact_sheet.language
  ORDER BY (sum(fact_sheet.viewer_count)) DESC;"""

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
        # cursor.execute(updatequery_17)
        # cursor.execute(updatequery_18)
        cursor.execute(updatequery_19)
        # cursor.execute(updatequery_20)
        cursor.execute(delete_view1)
        cursor.execute(delete_view2)
        cursor.execute(delete_view3)
        cursor.execute(create_view1)
        cursor.execute(create_view2)
        cursor.execute(create_view3)
        cursor.execute(create_view4)
        cursor.execute(create_view5)
        cursor.execute(create_view6)
        cursor.execute(create_view7)
        cursor.execute(create_view8)
        # cursor.execute(create_view9)
        cursor.execute(create_view10)
        cursor.execute(create_view11)
        cursor.execute(create_view12)
        cursor.execute(create_view13)
        cursor.execute(create_view14)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("execute_values() done")


def main():
    # Data Lake - Cleaned Zone
    ENDPOINT = "mightymufflonmovies.cmz4lzmucin2.us-east-1.rds.amazonaws.com"
    USERNAME = "lukas"
    PASSWORD = "lukas1234"
    DB_NAME = "mightymufflonmovies"

    cursor, conn = open_connection(ENDPOINT, USERNAME, PASSWORD, DB_NAME)
    df = get_data_from_dl_cleaned_zone(cursor, conn)
    close_connection(conn)

    # DWH
    ENDPOINT = "mightymufflondatawarehouse.cmz4lzmucin2.us-east-1.rds.amazonaws.com"
    USERNAME = "mightymufflon"
    PASSWORD = "mightydashboard"
    DB_NAME = "mightymufflondatawarehouse"
    cursor, conn = open_connection(ENDPOINT, USERNAME, PASSWORD, DB_NAME)
    load_data_to_dwh(cursor, conn, df)
    close_connection(conn)

