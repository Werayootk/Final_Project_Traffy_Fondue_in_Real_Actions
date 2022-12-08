import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.data.data_function import save_data_kmean

PATH_READ_RAW = "/dags/src/data/raw/"
PATH_SAVE_DATA = "/dags/src/data/output/"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='kmean_DAG',
    default_args=args,
    schedule_interval= '@daily',             # set interval
	catchup=False,                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)

task1 = PythonOperator(
    task_id='save_data_kmean',
    python_callable=save_data_kmean,        # function to be executed
    dag=dag,
    op_kwargs={ 'path_read': PATH_READ_RAW, 'path_save': PATH_SAVE_DATA }
)

task1