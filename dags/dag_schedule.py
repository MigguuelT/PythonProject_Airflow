from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'


def gera_arquivo():
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'w') as f:
        for i in range(10):
            f.write('Arquivo texto linha {}\n'.format(i))


def processa_arquivo():
    arquivo = ""
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'r') as f:
        for line in f:
            arquivo += line.strip('\n') + ' - Processado com sucesso!\n'

    with open(AIRFLOW_HOME + '/data/arquivo_processado.txt', 'w') as f:
        f.write(arquivo)

# data de inicio e data de fim, @daily, weekly, monthly ou yealy para agendar a dag
with DAG(
    dag_id = 'dag_schedule',
    schedule = '@monthly',
    catchup = False,
    start_date = datetime(2020, 1, 1),
    end_date = datetime(2026, 1, 1)
) as dag:

    gera_arquivo = PythonOperator(
        task_id = 'gera_arquivo',
        python_callable = gera_arquivo,
    )

    processa_arquivo = PythonOperator(
        task_id = 'processa_arquivo',
        python_callable = processa_arquivo,
    )

    gera_arquivo >> processa_arquivo
