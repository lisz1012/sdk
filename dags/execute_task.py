from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.snowflake.operator import SnowflakeOperator


with DAG(
    dag_id="execute_task",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    execute_task = SnowflakeOperator(
        task_id="execute_snowflake_task",
        sql='EXECUTE TASK task_db.public.insert_to_customer3',
        snowflake_conn_id='snowflake'
        # parameters={"id": 56},
    )
