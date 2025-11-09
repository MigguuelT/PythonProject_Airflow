import os
import csv
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import sqlite3

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

def read_data():
    # 1. Inicializa o Hook APENAS para acessar o objeto de conexão
    sqlite_hook = SqliteHook()
    sqlite_hook.sqlite_conn_id = 'meu_sqlite_local'  # 'ID' da conexão

    output_path = os.path.join(AIRFLOW_HOME, 'data', 'vendas.csv')  # Usando 'vendas' conforme seu teste

    print("Exportando dados com conexão nativa (solução de contorno)...")

    conn = None
    try:
        # OBTER O OBJETO DE CONEXÃO DO AIRFLOW
        airflow_conn_obj = sqlite_hook.get_connection('meu_sqlite_local')

        # PROCESSAR O URI (Host) PARA OBTER O CAMINHO PURO DO ARQUIVO
        # O campo .host contém a URI completa (ex: sqlite:////caminho/...)
        uri = airflow_conn_obj.host

        # Remove o prefixo para obter o caminho de arquivo que você sabe que funciona
        # A sintaxe de slicing [7:] funciona para 'sqlite://' e geralmente para 4 barras.
        # Vamos usar um método mais seguro:
        if uri.startswith('sqlite:////'):  # 4 barras (macOS/Linux)
            db_path = uri[10:]
        elif uri.startswith('sqlite:///'):  # 3 barras (Windows/Relativo)
            db_path = uri[10:]
        else:
            # Se for apenas o caminho direto, usa-o
            db_path = uri

        print(f"Caminho do DB extraído: {db_path}")

        # 2. USAR CONEXÃO NATIVA DO SQLITE COM O CAMINHO PURO
        conn = sqlite3.connect(db_path)

        # Executa a query
        cursor = conn.cursor()

        # Corrigindo para a tabela 'vendas' que você confirmou que existe
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

    except sqlite3.OperationalError as e:
        print(f"❌ Erro de Acesso ao Arquivo SQLite. REVERIFIQUE o caminho no Host da conexão: {e}")
        raise e
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado na DAG: {e}")
        raise e
    finally:
        if conn:
            conn.close()


# Use o nome de arquivo 'vendas.csv' para a entrada, pois é o nome usado pela Task 1

def copy_data():
    # INPUT: Deve ser o arquivo criado pela task1 (vendas.csv)
    input_file_name = 'vendas.csv'
    input_path = os.path.join(AIRFLOW_HOME, 'data', input_file_name)

    # OUTPUT: O novo arquivo transformado (vendas_new.csv)
    output_file_name = 'vendas_new.csv'
    output_path = os.path.join(AIRFLOW_HOME, 'data', output_file_name)

    lines = ''

    with open(input_path, 'r') as csvfile:
        for line in csvfile:
            lines += line.replace(',', '-')

    # A linha de escrita: agora usa output_path
    with open(output_path, 'w', newline='') as csvfile:
        # csv.writer é bom, mas como 'lines' é uma string grande, vamos usar .write()
        csvfile.write(lines)

    print(f"Dados processados de {input_file_name} e salvos em: {output_file_name}")


with DAG(
        dag_id="hook_dag_sqlite",  # Mude o ID da DAG para evitar conflitos
        schedule=None,
        start_date=datetime(2020, 1, 1),
        catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='read_data_from_sqlite',
        python_callable=read_data,
    )
    task2 = PythonOperator(
        task_id='copy_and_transform_data',
        python_callable=copy_data,
    )

    task1 >> task2
