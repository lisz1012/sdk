from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from include.datasets import MY_FILE


with DAG(
    dag_id='consumer',
    start_date=datetime(2022, 1, 1),
    schedule=[MY_FILE],
    catchup=False):

    @task
    def read_my_file():
        with open(MY_FILE.uri, 'r') as f:
            print(f.read())

    read_my_file()
