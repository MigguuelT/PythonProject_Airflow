from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import csv
import os
import sqlite3
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

def read_data():
    # 1. Inicializa o Hook. Se ele suportar, podemos passar o conn_id aqui.
    # Se ainda der erro de 'takes no arguments', volte para a inicialização sem args.
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id='meu_sqlite_local')
    except TypeError:
        # Fallback para a sintaxe que funcionou anteriormente
        sqlite_hook = SqliteHook()
        sqlite_hook.sqlite_conn_id = 'meu_sqlite_local'

    output_path = os.path.join(AIRFLOW_HOME, 'data', 'vendas.csv')

    conn = None
    try:
        # Tenta usar get_uri() para obter o caminho limpo, se disponível
        # Se não estiver disponível, usa get_conn() como antes.
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


def copy_data():
    input_file_name = 'vendas.csv'
    output_file_name = 'vendas_new.csv'

    input_path = os.path.join(AIRFLOW_HOME, 'data', input_file_name)
    output_path = os.path.join(AIRFLOW_HOME, 'data', output_file_name)

    lines = ''

    # Leitura
    with open(input_path, 'r') as csvfile:
        for line in csvfile:
            # Substitui todas as vírgulas por hífens
            lines += line.replace(',', '-')

    # Escrita: Usando newline='' é importante para csv, mas para escrever a string bruta, não é estritamente necessário,
    # mas mantemos a boa prática de abrir o arquivo corretamente.
    with open(output_path, 'w') as csvfile:
        csvfile.write(lines)

    print(f"Dados transformados e salvos em: {output_path}")


with DAG(
        dag_id="xcom_dag",
        schedule=None,
        start_date=datetime(2020, 1, 1),
        catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    task2 = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data,
    )

    task1 >> task2
