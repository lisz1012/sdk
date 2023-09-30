from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.snowflake.operator import SnowflakeOperator

execution_date = None
sql = None


def example(**context):
    execution_date = context['execution_date']
    print("execution_date is: ")
    print(execution_date)
    sql='%s %s'%('CALL CUSTOMERS_INSERT_PROCEDURE',  str(execution_date)[:10])
    execution_date = str(execution_date)[:10]
    print(sql)


with DAG(
    dag_id="call_procedure",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    task_example = PythonOperator(
        task_id='task_example',
        provide_context=True,
        python_callable=example
    )

    call_snowflake_proc = SnowflakeOperator(
        task_id="snowflake_op_with_params",
        sql="BEGIN;"
            "CALL TASK_DB.PUBLIC.CUSTOMER_INSERT_PROCEDURE (CREATE_DATE => '%s');"
            "COMMIT;" % execution_date,
        snowflake_conn_id='snowflake'
        # parameters={"id": 56},
    )

    task_example >> call_snowflake_proc
