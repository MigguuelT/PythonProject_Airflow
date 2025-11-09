from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def task2():
    print('Olá Mundo!')

with DAG(
    dag_id="operetor_dag",
    schedule=None,
    start_date=datetime(2020, 1, 1),
) as dag:

    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Olá Mundo!'",
    )

    task2 = PythonOperator(
        task_id="task2",
        python_callable=task2,
    )

    task1 >> task2
