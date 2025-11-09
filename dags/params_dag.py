from datetime import datetime
from airflow import DAG
from airflow.sdk import Param
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

with DAG(
    dag_id = 'params_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
    params= {
        'number_of_runs': 10,
        'added_value': Param(default=100,minimum=100,maximum=1000),
    }
) as dag:

    @task
    def task1(**context):
        added_value = context['params']['added_value']
        value = 0
        for i in range(context['params']['number_of_runs']):
            print(f'{i} - {value}')
            value += added_value

    task1()
