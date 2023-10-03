from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from include.datasets import MY_FILE


with DAG(
    dag_id="producer",
    schedule='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=False
):
    @task(outlets=[MY_FILE])
    def update_my_file():
        # None
        # Airflow监控的不是 uri 或者文件本身,而是 标记有outlets=这个 uri 的 task是否成功完成了,所以这里写 None 也会触发后面的consumer
        with open(MY_FILE.uri, "a+") as f:
            f.write("producer update")

    update_my_file()
