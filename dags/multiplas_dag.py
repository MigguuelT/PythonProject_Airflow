from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import csv
import os
import sqlite3
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'


# 1. Task Python
def task_saudacao():
    print("--- Task de Saudação ---")
    print("A DAG foi iniciada e os fluxos condicionais serão avaliados.")

# 2. Task Bash
def task_listagem():
    return f"ls -lh {AIRFLOW_HOME}/dags"

# 3. HookSqlite
def read_data():
    # 1. Inicializa o Hook. Se ele suportar, podemos passar o conn_id aqui.
    # Se ainda der erro de 'takes no arguments', volte para a inicialização sem args.
    try:
        sqlite_hook = SqliteHook(sqlite_conn_id='meu_sqlite_local')
    except TypeError:
        sqlite_hook = SqliteHook()
        sqlite_hook.sqlite_conn_id = 'meu_sqlite_local'

    output_path = os.path.join(AIRFLOW_HOME, 'data', 'vendas.csv')

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

# 4. Copiar para novo arquivo
def copy_data():
    input_file_name = 'vendas.csv'
    output_file_name = 'vendas_new.csv'

    input_path = os.path.join(AIRFLOW_HOME, 'data', input_file_name)
    output_path = os.path.join(AIRFLOW_HOME, 'data', output_file_name)

    lines = ''

    # Leitura
    with open(input_path, 'r') as csvfile:
        for line in csvfile:
            # Substitui todas as vírgulas por barras
            lines += line.replace(',', '/')

    with open(output_path, 'w') as csvfile:
        csvfile.write(lines)

    print(f"Dados transformados e salvos em: {output_path}")


# 5. Multiplicar o número dos produtos
def multiply_product_numbers():
    input_file_name = 'vendas.csv'
    output_file_name = 'vendas_atualizado.csv'  # Nome solicitado

    input_path = os.path.join(AIRFLOW_HOME, 'data', input_file_name)
    output_path = os.path.join(AIRFLOW_HOME, 'data', output_file_name)

    # Lista para armazenar todas as linhas processadas
    processed_rows = []

    print("--- Task de Multiplicação de Dados ---")

    try:
        with open(input_path, 'r', newline='') as infile:
            reader = csv.reader(infile)

            # Pula o cabeçalho, mas o armazena para a escrita
            header = next(reader)
            processed_rows.append(header)

            # Processa as linhas restantes
            for row in reader:
                # Assumindo que a coluna do número/quantidade do produto é a segunda (índice 1)
                if len(row) > 1:
                    try:
                        # Converte o valor para inteiro, multiplica por 2 e converte de volta para string
                        product_number = int(row[1]) * 2
                        row[1] = str(product_number)
                    except ValueError:
                        # Se não for um número, mantém o valor original (ex: se for uma string)
                        print(f"Aviso: Coluna 2 ('{row[1]}') não é um número. Mantendo o valor original.")

                processed_rows.append(row)

    except FileNotFoundError:
        print(f"ERRO: Arquivo de entrada não encontrado: {input_path}")
        raise

    # Escreve o novo arquivo CSV
    with open(output_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(processed_rows)

    print(f"Dados atualizados e salvos em: {output_file_name}")


with DAG(
    dag_id = 'multiplas_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    task1 = PythonOperator(
        task_id = 'task_saudacao',
        python_callable = task_saudacao,
    )

    task2 = BashOperator(
        task_id = 'task_listagem',
        bash_command = task_listagem(),
    )

    task3 = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    task4 = PythonOperator(
        task_id="copy_data",
        python_callable=copy_data,
    )

    task5 = PythonOperator(
        task_id="multiply_product_numbers",
        python_callable=multiply_product_numbers,
    )

    task1 >> [task2, task3] >> task4 >> task5

