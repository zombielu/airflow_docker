from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_with_aws_s3_v2',
    default_args=default_args,
    start_date=datetime(2026, 2, 10),
    schedule='@daily',
) as dag:
    task1 = S3KeySensor(
        task_id='sensor_s3',
        bucket_name='example110',
        bucket_key='data.csv',
        aws_conn_id='aws_s3',
        mode='poke',
        poke_interval=5, # this means check the file every 5 seconds.
        timeout=30, # checking within the 30-second time limit.
    )