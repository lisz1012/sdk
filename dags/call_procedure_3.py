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


with DAG(
    dag_id="call_procedure_3",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    task_example = PythonOperator(
        task_id='task_example',
        provide_context=True,
        python_callable=build_sql
    )


    call_snowflake_proc = SnowflakeOperator(
        task_id="snowflake_op_with_params",
        sql="""
                BEGIN;
                CALL TASK_DB.PUBLIC.CUSTOMER_INSERT_PROCEDURE (CREATE_DATE => '{{ execution_date }}');
                COMMIT;
            """,
        snowflake_conn_id='snowflake'
    )

    task_example >> call_snowflake_proc
