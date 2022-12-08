import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data.kafka_producer import load_data_init
from src.data.kafka_consumer import get_data_init

from datetime import datetime
import requests
import json
import time
import pandas as pd
import numpy as np
import io
import pickle
import os
import logging

PATH_INIT = "/dags/src/data/raw/"
PATH_OUTPUT = "/dags/src/data/output/"
PATH_SCHEMA = "/dags/src/avro/"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='initial_load_data_DAG',
    default_args=args,
    schedule_interval= '@once',             # set interval
	catchup=False,                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)

task1 = PythonOperator(
    task_id='load_data_init',
    python_callable=load_data_init,        # function to be executed
    dag=dag,
    op_kwargs={'path_init': PATH_INIT, 'path_output': PATH_OUTPUT, 'path_avsc': PATH_SCHEMA}
)

task2 = PythonOperator(
    task_id='get_data_init',
    python_callable=get_data_init,
    dag=dag,
    op_kwargs={'path_init': PATH_INIT, 'path_output': PATH_OUTPUT, 'path_avsc': PATH_SCHEMA}
)

task1 >> task2                  # set task priority
