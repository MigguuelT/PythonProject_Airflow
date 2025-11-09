from datetime import datetime

from airflow import DAG
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

with DAG(
    dag_id = 'pool_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    @task
    def task_default_pool():
        print("task_default_pool")

    @task(pool='pool_teste1')
    def task_pool1():
        print("task_pool1")

    @task(pool='pool_teste2')
    def task_pool2():
        print("task_pool2")

    @task(pool='pool_teste1')
    def task_pool1_2():
        print("task_pool1_2")

    @task(pool='pool_teste2')
    def task_pool2_2():
        print("task_pool2_2")

    task_default_pool() >> [task_pool1(), task_pool1_2()] >> task_default_pool() >> [task_pool2(), task_pool2_2()]
