from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers import functions
import sys
import os


default_args = {
    'owner': 'prabhu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    default_args=default_args,
    dag_id='trends_dag',
    description='google trends dag',
    start_date=datetime(2022, 10, 2),
    schedule_interval='@daily'
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=functions.extract,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=functions.transform,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=functions.load,
    )

    end = PythonOperator(
        task_id='end',
        python_callable=functions.end,
    )


    extract >> transform >> load >> end
