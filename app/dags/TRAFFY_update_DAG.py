import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data.kafka_producer import load_data_daily
from src.data.kafka_consumer import update_data_daily

PATH_UPDATE = "/dags/src/data/raw/"
PATH_SCHEMA = "/dags/src/avro/"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),   
    'provide_context': True,                           
}

dag = DAG(
    dag_id='update_load_data_DAG',
    default_args=args,
    schedule_interval= '@daily',             
	catchup=False,                          
)

task1 = PythonOperator(
    task_id='load_data_daily',
    python_callable=load_data_daily,        
    dag=dag,
    op_kwargs={ 'path_update': PATH_UPDATE, 'path_schema': PATH_SCHEMA }
)

task2 = PythonOperator(
    task_id='update_data_daily',
    python_callable=update_data_daily,        
    dag=dag,
    op_kwargs={ 'path_update': PATH_UPDATE, 'path_schema': PATH_SCHEMA }
)

task1 >> task2
