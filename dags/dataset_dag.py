import csv
import os
import sqlite3
from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Dataset

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

novo_csv = Dataset('file://' + AIRFLOW_HOME + '/data/novo.csv')

def read_sql():
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id='meu_sqlite_local')
    except TypeError:
        sqlite_hook = SqliteHook()
        sqlite_hook.sqlite_conn_id = 'meu_sqlite_local'

    output_path = os.path.join(AIRFLOW_HOME, 'data', 'novo.csv')

    conn = None
    try:
        # Tenta usar get_uri() para obter o caminho limpo, se disponível
        # Se não estiver disponível, usa get_conn() .
        db_path = sqlite_hook.get_uri().replace('sqlite:///', '').replace('sqlite:////', '')

        # 2. CONEXÃO DIRETA com o caminho LIMPO
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query (assumindo que 'vendas' existe)
        cursor.execute("SELECT * FROM vendas")
        records = cursor.fetchall()

        # Obtém o cabeçalho
        header = [i[0] for i in cursor.description] if cursor.description else []

        # Escreve o CSV
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if header:
                writer.writerow(header)
            writer.writerows(records)

        print(f"Dados exportados para: {output_path}")

    except Exception as e:
        print(f"Erro durante a exportação: {e}")
        raise e
    finally:
        if conn:
            conn.close()


with DAG(
    dag_id = 'dataset_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:
    task1 = PythonOperator(
        task_id = 'read_sql',
        python_callable = read_sql,
        outlets=[novo_csv],
    )
    task1

