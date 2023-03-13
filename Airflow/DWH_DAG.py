
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl_dwh import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,12,7),
    'email': ["daniel.podolecki@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dwh_dag_01',
    default_args=default_args,
    description='DAG to load data Data Lake to Data Ware House!',
    schedule_interval=timedelta(days=1)
)


run_etl = PythonOperator(
    task_id='dwh_dag',
    python_callable=main,
    dag=dag
)

run_etl

