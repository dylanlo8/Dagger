from airflow.models import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from zipfile import ZipFile 
import pandas as pd
import json
from google.cloud import bigquery

client = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["lojingjiedylan@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='is3107_groupproject', default_args=default_args, schedule=None, catchup=False, tags=['is3107'])

def assignment3_dag():
    @task
    def extract_data():
        pass

    @task
    def transform_coffee_data(filepath):
        pass
    
    @task
    def generate_coffee_report(json_path):
        pass
        

    # Task dependencies (DAG)
    # filepath = extract_survey_data()
    # json_path = transform_coffee_data(filepath)
    # generate_coffee_report(json_path)


assignment3_etl = assignment3_dag()
