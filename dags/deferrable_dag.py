from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.time import TimeSensorAsync

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

def task():
    print('Olá!')

with DAG(
    dag_id = 'deferrable_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    sensor = TimeSensorAsync(
        task_id = 'sensor',
        target_time=datetime(2025, 10, 23).time()
    )

    task1 = PythonOperator(
        task_id = 'task1',
        python_callable = task
    )

    sensor >> task1
