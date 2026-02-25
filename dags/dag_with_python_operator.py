from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f'Hello! My name is {first_name} {last_name} '
          f'and I am {age} years old!')

def get_name(ti):
    ti.xcom_push(key='first_name', value='Amy')
    ti.xcom_push(key='last_name', value='Friday')

def get_age(ti):
    ti.xcom_push(key='age', value=24)

with DAG(
    dag_id='our_dag_with_python_operator_v5',
    default_args=default_args,
    description='dag using python operator',
    start_date=datetime(2026, 2, 1),
    schedule='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,

    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
    )
    [task2, task3] >> task1