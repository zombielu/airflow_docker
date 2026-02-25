from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='dag_with_taskflow_api_v3',
    default_args=default_args,
    description='dag using python operator',
    start_date=datetime(2026, 2, 1),
    schedule='@daily',
)
def hello_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'John',
            'last_name': 'Doe',
        }

    @task()
    def get_age():
        return 3

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello, {first_name} {last_name}! Your age is {age}")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greeting_dag = hello_etl()

