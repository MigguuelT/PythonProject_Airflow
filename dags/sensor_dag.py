import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

def read_files():
    # A task de leitura deve ler especificamente a pasta 'data/teste'
    data_dir = os.path.join(AIRFLOW_HOME, 'data', 'teste')
    print(f"Listando arquivos em: {data_dir}")
    for path in os.listdir(data_dir):
        print(path)

with DAG(
    dag_id = 'sensor_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
    catchup=False,
) as dag:

    # 1. CORREÇÃO: Usando 'filepath' e um padrão
    # O sensor irá monitorar: [HOST da FS_CONN] + [filepath]
    # No seu caso: /.../PythonProject_Airflow/ + data/*
    sensor = FileSensor(
        task_id = 'filesensor',
        # 'data/*' diz ao sensor para procurar qualquer arquivo dentro da pasta 'data'
        filepath = 'data/teste/*',
        fs_conn_id='FS_CONN',
        # O parâmetro recursive garante que ele procurará dentro da pasta
        recursive=True,
        poke_interval=5,
        timeout = 300,
    )

    read_files_task = PythonOperator(
        task_id = 'read_files',
        python_callable = read_files,
    )

    sensor >> read_files_task