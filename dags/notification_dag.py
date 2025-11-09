from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'


def task1():
    print('Task 1')

def success_callback(**kwargs):
    print('Sucesso!')

def failure_callback(**kwargs):
    print('Fail!')


with DAG(
    dag_id='notification_dag',
    schedule='@once',
    default_args={
        'email': 'teste@teste.com',
        'email_on_success': True,
        'email_on_failure': True,
        'email_on_retry': True,
    },
    start_date=datetime(2020, 1, 1),
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
):

    task = PythonOperator(
        task_id='task1',
        python_callable=task1,
    )

    task
