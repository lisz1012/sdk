from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.snowflake.operator import SnowflakeOperator

execution_date = None
sql = None


def build_sql(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    sql_statement = """
        BEGIN;
        CALL TASK_DB.PUBLIC.CUSTOMER_INSERT_PROCEDURE (CREATE_DATE => '%s');
        COMMIT;
    """ % execution_date
    context['ti'].xcom_push(key='dynamic_sql', value=sql_statement)


def print_sql(**context):
    sql = context['ti'].xcom_pull(task_ids='task_example', key='dynamic_sql')
    print("sql is: %s" % sql)


with DAG(
    dag_id="call_procedure_2",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    task_example = PythonOperator(
        task_id='task_example',
        provide_context=True,
        python_callable=build_sql
    )

    print_query = PythonOperator(
        task_id='print_query',
        provide_context=True,
        python_callable=print_sql
    )

    call_snowflake_proc = SnowflakeOperator(
        task_id="snowflake_op_with_params",
        sql="{{ task_instance.xcom_pull(task_ids='build_sql', key='dynamic_sql') }}",
        snowflake_conn_id='snowflake'
    )

    task_example >> print_query >> call_snowflake_proc
