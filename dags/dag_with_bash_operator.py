from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='An example DAG',
    start_date=datetime(2026, 2, 1),
    schedule='@daily',
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World!"',
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo I am task2, I am running after task1',
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='echo I am task3, I am running after task1 and with task2 at the same time',
    )

    # task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task dependency method 2
    # task1 >> task2
    # task1 >> task3

    # task dependency method 3
    task1 >> [task2, task3]