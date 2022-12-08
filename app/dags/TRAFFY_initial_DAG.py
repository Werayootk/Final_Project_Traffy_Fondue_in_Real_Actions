import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data.kafka_producer import load_data_init
from src.data.kafka_consumer import get_data_init


PATH_INIT = "/dags/src/data/raw/"
PATH_OUTPUT = "/dags/src/data/output/"
PATH_SCHEMA = "/dags/src/avro/"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),   
    'provide_context': True,                           
}

dag = DAG(
    dag_id='initial_load_data_DAG',
    default_args=args,
    schedule_interval= '@once',             
	catchup=False,                          
)

task1 = PythonOperator(
    task_id='load_data_init',
    python_callable=load_data_init,        
    dag=dag,
    op_kwargs={'path_init': PATH_INIT, 'path_output': PATH_OUTPUT, 'path_avsc': PATH_SCHEMA}
)

task2 = PythonOperator(
    task_id='get_data_init',
    python_callable=get_data_init,
    dag=dag,
    op_kwargs={'path_init': PATH_INIT, 'path_output': PATH_OUTPUT, 'path_avsc': PATH_SCHEMA}
)

task1 >> task2                  
