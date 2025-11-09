import csv
import os
import sqlite3
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.python import PythonOperator

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'


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
        cursor.execute("SELECT * FROM tabela_produtos")
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


def transform_data(AIRFLOW_HOME: str):

    # 1. DEFINIR OS CAMINHOS COMPLETOS
    input_path = os.path.join(AIRFLOW_HOME, 'data', 'novo.csv')
    output_path = os.path.join(AIRFLOW_HOME, 'data', 'novo_modified.csv')

    # O diretório de destino é o pai do arquivo de saída
    output_dir = os.path.dirname(output_path)

    print(f"Diretório de saída esperado: {output_dir}")

    # 2. CRIAÇÃO DO DIRETÓRIO SE NÃO EXISTIR
    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Diretório '{output_dir}' verificado/criado com sucesso.")
    except OSError as e:
        print(f"Erro ao criar diretório {output_dir}: {e}")
        raise

    # 3. LEITURA DOS DADOS
    try:
        data = pd.read_csv(input_path)
        print(f"CSV lido com sucesso de: {input_path}")
    except FileNotFoundError:
        print(f"ERRO: Arquivo de entrada não encontrado em: {input_path}")
        raise

    # 4. TRANSFORMAÇÃO
    data['preco'] = data['preco'].apply(lambda x: x + 100)
    print("Preços transformados com sucesso.")

    # 5. ESCRITA DOS DADOS
    data.to_csv(output_path, index=False)
    print(f"Arquivo modificado salvo em: {output_path}")

def clean_data():
    os.remove(AIRFLOW_HOME + '/data/vendas_read.csv')


with DAG(
    dag_id = 'cleanup_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    task1 = PythonOperator(
        task_id = 'read_sql',
        python_callable = read_sql,
    )
    task2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={
            'AIRFLOW_HOME': AIRFLOW_HOME
        }
    )
    task3 = PythonOperator(
        task_id='cleanup_data',
        python_callable=clean_data,
        trigger_rule='all_done',
    )

    task1 >> task2 >> task3
