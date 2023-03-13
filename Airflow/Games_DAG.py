from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl_airflow_02 import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,11,2),
    'email': ["daniel.podolecki@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'steam_dag_02',
    default_args=default_args,
    description='DAG to load data from Steam API to S3!',
    #schedule_interval=timedelta(days=1),
)


run_etl = PythonOperator(
    task_id='complete_steam_etl_02',
    python_callable=main,
    dag=dag,
)

run_etl