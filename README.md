# mightymufflon
Welcome to our Semester Project for the Module Data Warehouse and Data Lake Systems. This project was conducted by Joel Hirschi, Lukas Niederhäuser and Daniel Podolecki.

It is made for the Indie Game Developer Team "Mighty Mufflon Games". The goal is to give an overview about the video game market based on several sources and to give a possibility to estimate the revenue based on input parameters.

Note this is a clone from the original project: https://github.com/Harigata/mightymufflon


##  General Setup

For this project you will need to have an Amazon account and you need to configure three S3 Buckets for the Data Lake Raw Zone, one RDS for the Data Lake Cleaned Zone and one RDS for the Data Warehouse. Additionally you will need to have a Virtual Machine in order to run Airflow on it. For that you can use EC2 in the AWS Environment or you can come up with your own VM solution.

- S3 Configuration: Please set the Block all public access to "Off" and the access to public. Keep the standard settings for the rest of the options.
- RDS Configuration: Please set the Public Accessibility to Yes, Engine Version = PostgresSQL 13.7-R1, Dev-Test Instance, Micro Version.
- EC2 Configuration: You can follow these steps to initializie a EC2 instance in order to run Apache Airflow in the AWS environment: https://www.youtube.com/watch?v=q8q3OFFfY6c&amp;t=2104s


## Sources: Ingestion to Data Lake (Raw and Cleaned Zone)
--- <b> Scraping  </b> ---

To get the wikipedia data a scraper has to be implemented on the following links:

- https://en.wikipedia.org/wiki/List_of_video_game_developers

- https://en.wikipedia.org/wiki/List_of_indie_game_developers

The code behind the Wikipedia Scraper can be found here: https://github.com/Harigata/mightymufflon/blob/main/Scraping/my_script_wiki.R

To get the steamstore webpage data a scraper has to be implemented on the following link:

https://store.steampowered.com/search/?category1=998%2C21&filter=topsellers"

The code behind the Steamstore Scraper can be found here: https://github.com/Harigata/mightymufflon/blob/main/Scraping/my_script_steamstore.R

The Scraping Scripts has to be scheduled. With the Programming Language this can be done with the taskscheduler package. The code can be found here: https://github.com/Harigata/mightymufflon/blob/main/Scraping/taskscheduler.R

The ouput of the scrapers are CSVs. To upload the CSVs to a S3 Bucket (DL Raw Zone) you can use this code: 
https://github.com/Harigata/mightymufflon/blob/main/PyCharm/Games_Upload_CSVs.py

To clean the data from the S3 bucket (Raw Zone) to RDS (Cleaned Zone). You can use Lamba Function according to this code:
https://github.com/Harigata/mightymufflon/blob/main/Lambda/Games_ETL_Cleaned_Zone.py
Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

In the Lamba Functions you will need to install third party packages (pandas, numpy, psycopg2-binary and requests) on a EC2 Instance and insert this as a layer. For details see: https://gist.github.com/jose0628/377f7a24650844474155f8cddfb6f665


--- <b> CSV Flat File </b> ----

To get the game_achievements CSV click on this link and download it:

www.arstechnica.com/wp-content/uploads/2018/07/games_achievements_players_2018-07-01.csv

To upload the CSV to a S3 Bucket (DL Raw Zone) you can use this code: 
https://github.com/Harigata/mightymufflon/blob/main/PyCharm/Games_Upload_CSVs.py

To clean the data from the S3 bucket (Raw Zone) to RDS (Cleaned Zone). You can use Lamba Function according to this code:
https://github.com/Harigata/mightymufflon/blob/main/Lambda/Games_ETL_Cleaned_Zone.py
Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

In the Lamba Functions you will need to install third party packages (pandas, numpy, psycopg2-binary and requests) on a EC2 Instance and insert this as a layer. For details see: https://gist.github.com/jose0628/377f7a24650844474155f8cddfb6f665

-- <b> Steamspy API </b> ---

To get access to the list of all games use the following parameters in a requests.get command:

url = "https://steamspy.com/api.php"
parameters = {"request": "all"}

--> requests.get(url=url, params=parameters)

To get access to the detailed information per game for the steamspy source it is recommended to iterate through a list and insert an steam_app_id in each iteration with the following logic:

API_URL_DETAILED = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"

To ingest the Steamspy API data to the S3 bucket (Raw Zone) one can use Apache Airflow or Lamba Function. The Python Extraction code and the corresponding DAG can be found here:
- Airflow Extraction: https://github.com/Harigata/mightymufflon/blob/main/Airflow/Games_ETL_Raw_Zone.py 
- Airflow DAG: https://github.com/Harigata/mightymufflon/blob/main/Airflow/Games_DAG.py
- Lambda Extraction: https://github.com/Harigata/mightymufflon/blob/main/Lambda/Games_ETL_Raw_Zone.py

Note that the Lamba function needs a bit more power than the standard settings provide. Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

In the Lamba Functions you will need to install third party packages (pandas, numpy, psycopg2-binary and requests) on a EC2 Instance and insert this as a layer. For details see: https://gist.github.com/jose0628/377f7a24650844474155f8cddfb6f665

To clean the data from the S3 bucket (Raw Zone) to RDS (Cleaned Zone). You can use Lamba Function according to this code:
https://github.com/Harigata/mightymufflon/blob/main/Lambda/Games_ETL_Cleaned_Zone.py
Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

For more details to the Steamspy API see the documentation: 

https://steamspy.com/api.php


-- <b> Steam API </b> ---

To get access to the list of all games use the following parameters in a requests.get command:

url = "https://steamspy.com/api.php"
parameters = {"request": "all"}

--> requests.get(url=url, params=parameters)

To get access to the detailed information per game for the steam source it is recommended to iterate  through a list and insert an steam_app_id in each iteration with the following logic:

API_URL_DETAILED = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

To ingest the Steam API data to the S3 bucket (Raw Zone) one can only use Apache Airflow because the timeout is longer than 15 minutes and therefore Amazon Lambda Function cannot extract the Data from the Steam API. The Python Extraction code and the corresponding DAG can be found here:
- Airflow Extraction: https://github.com/Harigata/mightymufflon/blob/main/Airflow/Games_ETL_Raw_Zone.py 
- Airflow DAG: https://github.com/Harigata/mightymufflon/blob/main/Airflow/Games_DAG.py

Note that the Lamba function needs a bit more power than the standard settings provide. Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

In the Lamba Functions you will need to install third party packages (pandas, numpy, psycopg2-binary and requests) on a EC2 Instance and insert this as a layer. For details see: https://gist.github.com/jose0628/377f7a24650844474155f8cddfb6f665

To clean the data from the S3 bucket (Raw Zone) to RDS (Cleaned Zone). You can use Lamba Function according to this code:
https://github.com/Harigata/mightymufflon/blob/main/Lambda/Games_ETL_Cleaned_Zone.py
Please boost your Lamba Function under "Configuration" with the following parameters:
- Timeout = 15 mins, 0 sec
- Memory = 1000 MB
- Ephemeral storage = 512MB

For more details to the Steam API see the documentation:

- https://partner.steamgames.com/doc/webapi_overview#1 

- https://wiki.teamfortress.com/wiki/WebAPI/GetAppList 


-- <b> Twitch API  </b> --

A developer account on twitch is strictly needed for the using of the API. An account can be created under the following link: https://dev.twitch.tv/
After creating the account you have to create a project under your profile. proceed as follows: 
1. give your project a name of your choice. 
2. enter the following entry as OAuth Redirect URLs: http://localhost:3000
3. under category, select Analytics Tool. 
4. generate a client_secret_key

Important: You need the client key and the client secret key for the data query in the code Twitch_Raw_Zone.py. Add the keys in the firts section of the code.  

-- <b> Movie Database API  </b> ---

To get access to the Movie Database API, one has to register at: https://developers.themoviedb.org/3/getting-started/introduction and follow the instructions. Following requests are made: 

- https://api.themoviedb.org/3/movie/top_rated?api_key=<<api_key>>&language=en-US&page=1 (get the top rated movies) 

for the series the following request is made: 

- https://api.themoviedb.org/3/tv/top_rated?api_key=<<api_key>>&language=en-US&page=1 (get the top rated series). 

Also the genres have to be extracted via these two calls: 

- https://api.themoviedb.org/3/genre/movie/list?api_key=<<api_key>>&language=en-US (movie-genres)
- https://api.themoviedb.org/3/genre/tv/list?api_key=<<api_key>>&language=en-US (series-genres).

For all the API-Calls the same API-Key can be used. The script for the API-Extraction and storage in the raw-zone is visible here: Movie_Series_Raw_Zone.py in the folder called "Lambda".

## Ingestion to Data Warehouse 

-- <b> Datawarehouse Ingestion  </b> ---

For the Datawarehouse Ingestion you need to have a VM in order to run Apache Airflow on it. 
The Python Execution code and the corresponding DAG can be found here:

- Airflow Execution: https://github.com/Harigata/mightymufflon/blob/main/Airflow/DWH_ETL.py
- Airflow DAG: https://github.com/Harigata/mightymufflon/blob/main/Airflow/DWH_DAG.py


## Visualisation / Business Intelligence

-- <b> Tableau Visualisation </b> ---

Click on the link to explore Tableau Visualisation which are uploaded on the Tableau Public Cloud: 
https://public.tableau.com/app/profile/daniel.podolecki/viz/EricBalashTableauResumeV2_16717103154680/Home
