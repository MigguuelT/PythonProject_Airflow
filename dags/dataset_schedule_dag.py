import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Dataset

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

novo_csv = Dataset('file://' + AIRFLOW_HOME + '/data/novo.csv')

def read_dataset():
    df = pd.read_csv(os.path.join(AIRFLOW_HOME, 'data/novo.csv'))

with DAG(
    dag_id = 'dataset_schedule_dag',
    schedule = [novo_csv],
    start_date = datetime(2020, 1, 1),
) as dag:
    task1 = PythonOperator(
        task_id = 'read_dataset',
        python_callable = read_dataset,
    )
    task1
