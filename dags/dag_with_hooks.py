from datetime import datetime, timedelta
import csv
import logging
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def postgres_to_s3(**context):
    # step 1: query data from postgresql db save it as a text file
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        select *
        from output
        where date >= %s and date <= %s
        """,
        (start.date(), end.date())
    )

    with NamedTemporaryFile(mode='w', suffix=f'_{start.date()}.txt') as f: # ✅ with 结束 = 自动 flush + close + 文件自动删除
        csvwriter = csv.writer(f)
        # write column name as the first row
        csvwriter.writerow([i[0] for i in cursor.description])
        # then write all the rest data in rows 流式写数据
        csvwriter.writerows(cursor)
        # 从python 缓存->OS 缓冲区->磁盘，当你需要“立刻让别的代码 / 进程 / 系统读到文件内容”时，就要 flush()
        # flush完还可以继续写
        f.flush()

        cursor.close()
        conn.close()
        # Note: f.name is the path of the temp file
        logging.info(f'!!! Save orders data in text file: %s', f.name)
        # logging info will be printed in the dag run log on the airflow UI.

    # step 2: upload the text file into S3
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        s3_hook.load_file(
            filename = f.name,
            key = f"orders/{start.date()}.txt",
            bucket_name = 'example110',
            replace = True,
        )
        logging.info(f'!!! Save text file to S3: %s', f.name)


with DAG(
    dag_id='our_dag_with_hooks_v20',
    default_args=default_args,
    description='dag using python operator',
    start_date=datetime(2026, 2, 22),
    schedule='@daily',
    catchup=True,
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3,
    )