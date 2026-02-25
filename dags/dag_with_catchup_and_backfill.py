from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v9',
    default_args=default_args,
    description='An example DAG',
    start_date=datetime(2026, 2, 12),
    schedule='@daily',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World!"',
    )