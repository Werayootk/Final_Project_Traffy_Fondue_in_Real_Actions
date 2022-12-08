import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data.data_function import save_data_kmean

PATH_READ_RAW = "/dags/src/data/raw/"
PATH_SAVE_DATA = "/dags/src/data/output/"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      
    'provide_context': True,                            
}

dag = DAG(
    dag_id='kmean_DAG',
    default_args=args,
    schedule_interval= '@daily',             
	catchup=False,                          
)

task1 = PythonOperator(
    task_id='save_data_kmean',
    python_callable=save_data_kmean,        
    dag=dag,
    op_kwargs={ 'path_read': PATH_READ_RAW, 'path_save': PATH_SAVE_DATA }
)

task1