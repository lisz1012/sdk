from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.snowflake.operator import SnowflakeOperator


with DAG(
    dag_id="call_procedure_4",
    schedule="@daily",
    start_date=datetime(2023, 9, 27),
    catchup=False
):

    call_snowflake_proc = SnowflakeOperator(
        task_id="snowflake_op_with_params",
        sql="""
                BEGIN;
                CALL TASK_DB.PUBLIC.CUSTOMER_INSERT_PROCEDURE (CREATE_DATE => '{{ execution_date }}');
                COMMIT;
            """,
        snowflake_conn_id='snowflake'
    )
