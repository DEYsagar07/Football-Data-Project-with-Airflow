# import os
import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, load_wikipedia_data

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Sagar Dey",
        "start_date": datetime(2024, 11, 8),
    },
    schedule_interval=None,
    catchup=False
)

extract_data_from_wikipedia = PythonOperator(
    task_id='extract_data_from_wikipedia',
    python_callable=extract_wikipedia_data,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    python_callable=transform_wikipedia_data,
    dag=dag
)

load_wikipedia_data = PythonOperator(
    task_id='load_wikipedia_data',
    python_callable=load_wikipedia_data,
    dag=dag
)

# Set up the task dependencies
extract_data_from_wikipedia >> transform_wikipedia_data >> load_wikipedia_data
