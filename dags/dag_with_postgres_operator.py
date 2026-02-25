from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_with_postgres_operator_v2',
    default_args=default_args,
    description='An example DAG',
    start_date=datetime(2026, 2, 1),
    schedule='0 0 * * *',
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """,
    )

    task2 = SQLExecuteQueryOperator(
        task_id='insert_value',
        conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ds}}', '{{dag.dag_id}}')
        """,
        # ds is dag run's execution date, ds and dag_id are set by default by
        # airflow engine and can be accessed by putting the variable name into
        # two curly brackets.
        # ds and dag are airflow macros, you can find them
        # https://airflow.apache.org/docs/apache-airflow/1.10.13/macros-ref.html
    )

    task1 >> task2