from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    print("Hello World!")

with DAG(
    dag_id = "hello_world",
    schedule_interval = "@daily",
    default_args = {
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2022,8,22)
    },catchup = False) as dag:

            hello_world = PythonOperator(
                task_id='hello_world',
                python_callable= hello_world)
    
hello_world