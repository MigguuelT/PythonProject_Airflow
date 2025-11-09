import time
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def t1():
    print('Task 1')

def t2():
    print('Task 2')
    time.sleep(2)

def t3():
    print('Task 3')
    time.sleep(3)

def t4():
    print('Task 4')
    time.sleep(3)

def t5():
    print('Task 5')
    time.sleep(1)

with DAG(
        dag_id="execucao_dag",
        schedule=None,
        start_date=datetime(2020, 1, 1),
        catchup=False,
) as dag:

    # 1. TAREFAS RENOMEADAS E CORRIGIDAS:
    # A task11 estava usando o nome da task importada (task1), causando o conflito.
    # Vamos usar um nome claro e a função local t1.
    task_t1 = PythonOperator(
        task_id="t1",
        python_callable=t1, # Usa a função t1() local
    )
    task2 = PythonOperator(
        task_id="t2",
        python_callable=t2,
    )
    task3 = PythonOperator(
        task_id="t3",
        python_callable=t3,
    )
    task4 = PythonOperator(
        task_id="t4",
        python_callable=t4,
    )
    task5 = PythonOperator(
        task_id="t5",
        python_callable=t5,
    )

    # 2. FLUXO CORRIGIDO:
    # Usando o nome da variável de task localmente definida (task_t1)
    task_t1 >> [task2, task3] >> task4 >> task5