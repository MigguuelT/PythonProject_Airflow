import os
import shutil
import pendulum
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.sdk import Param
from airflow.sdk.definitions.decorators import task

# --- VARIÁVEIS DE AMBIENTE E CAMINHOS ---
AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
ETL_DIR = os.path.join(DATA_DIR, 'etl')
RESULTS_DIR = os.path.join(ETL_DIR, 'results')
JSON_DIR = os.path.join(ETL_DIR, 'json')
CSV_DIR = os.path.join(ETL_DIR, 'csv')
SQL_DIR = os.path.join(ETL_DIR, 'sql')

# --- CONFIGURAÇÃO DE TENTATIVAS (RETRIES) ---
default_args = {
    'owner': 'airflow',
    'retries': 2,  # Tenta rodar a task 2 vezes (total de 3 tentativas)
    'retry_delay': timedelta(minutes=1),  # Espera 1 minuto entre as tentativas
    'execution_timeout': timedelta(minutes=3),  # Tempo máximo que a task pode rodar
}


# --- FUNÇÕES DAS TASKS ---

@task
def ensure_local_data_exists():
    """Garante que os diretórios de trabalho existam para I/O local."""
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(SQL_DIR, exist_ok=True)
    print("Diretórios de trabalho locais garantidos.")


@task
def load_metadata_local():
    """Simula o carregamento de metadados JSON localmente."""
    print(f"Metadados JSON existindo nos caminhos: {JSON_DIR}.")


@task
def load_csv_local(**context):
    """
    Simula o carregamento de CSVs locais para o período definido pelos params.
    """
    df_list = []
    start_date = context['params']['start_date']
    end_date = context['params']['end_date']

    print(f"Iniciando checagem de CSVs de {start_date} a {end_date}...")

    for date in pd.date_range(start_date, end_date):
        file_path = os.path.join(CSV_DIR, f'data_{date.strftime("%Y-%m-%d")}.csv')

        if os.path.exists(file_path):
            df_list.append(file_path)
            print(f"Arquivo CSV encontrado: {file_path}")
        else:
            print(f"AVISO: Arquivo não encontrado - {file_path}. Ignorando.")

    if not df_list:
        # Permite que o DAG continue mesmo sem dados, mas o merge pode falhar depois.
        print("AVISO: Nenhum CSV encontrado no período.")
        return []

    # Retorna a lista de caminhos dos arquivos encontrados para a próxima task
    return df_list


@task
def merge_csv(csv_paths, **context):
    """Mescla a lista de arquivos CSV fornecida e salva o resultado."""

    if not csv_paths:
        raise ValueError("Nenhum arquivo CSV encontrado para mesclar.")

    # Lê todos os DataFrames e concatena
    df_list = [pd.read_csv(f) for f in csv_paths]
    df = pd.concat(df_list, ignore_index=True)

    output_path = os.path.join(RESULTS_DIR, 'main_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Mesclagem de CSV concluída. Salvo em: {output_path}")

    return output_path  # Retorna o caminho do CSV principal


@task
def transform_data(main_csv_path):
    """Aplica a lógica de merge de metadados ao arquivo principal."""

    main_df = pd.read_csv(main_csv_path)

    # Leituras de JSON
    product_df = pd.read_json(os.path.join(JSON_DIR, 'products.json'))
    store_df = pd.read_json(os.path.join(JSON_DIR, 'stores.json'))
    salesperson_df = pd.read_json(os.path.join(JSON_DIR, 'salesperson.json'))

    # Lógica de transformação
    product_df.rename(columns={'id': 'product_id'}, inplace=True)
    salesperson_df.rename(columns={'id': 'salesperson_id'}, inplace=True)

    final_df = main_df.merge(product_df, left_on='product', right_on='product_id', how='left')
    final_df = final_df.merge(store_df, left_on='store', right_on='store_id', how='left')
    final_df = final_df.merge(salesperson_df, left_on='salesperson', right_on='salesperson_id', how='left')

    output_path = os.path.join(RESULTS_DIR, 'final_data.csv')
    final_df.to_csv(output_path, index=False)
    print(f"Transformação concluída. Salvo em: {output_path}")

    return output_path  # Retorna o caminho do CSV final


@task(task_id='insert_table')
def insert_data_to_sqlite(final_csv_path):
    """Lê o CSV final, extrai os dados e usa hook.insert_rows para inserção em massa."""

    if not final_csv_path or not os.path.exists(final_csv_path):
        raise FileNotFoundError(f"Arquivo final não encontrado para inserção: {final_csv_path}")

    df = pd.read_csv(final_csv_path)

    target_fields = ['id', 'name', 'state', 'store_name', 'store_adress', 'product_name', 'salesperson_id']
    data_to_insert = df[target_fields].values.tolist()

    hook = SqliteHook(sqlite_conn_id='sqlite_default')

    hook.insert_rows(
        table='sales',
        rows=data_to_insert,
        target_fields=target_fields,
        replace=True
    )
    print(f"Dados inseridos no SQLite usando insert_rows. Total de {len(data_to_insert)} linhas.")


@task(trigger_rule='all_done')
def cleanup_data():
    """Limpa APENAS os diretórios de saída (results e sql) APÓS a conclusão do processo."""

    print("Iniciando limpeza de arquivos temporários e de saída...")

    # Limpa resultados temporários
    if os.path.exists(RESULTS_DIR):
        shutil.rmtree(RESULTS_DIR, ignore_errors=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Limpa scripts SQL
    if os.path.exists(SQL_DIR):
        shutil.rmtree(SQL_DIR, ignore_errors=True)
    os.makedirs(SQL_DIR, exist_ok=True)

    print(f"Diretórios de saída ({RESULTS_DIR} e {SQL_DIR}) limpos.")


# --- DEFINIÇÃO DA DAG ---

with DAG(
        dag_id='pipeline_etl_retry_dag',
        default_args=default_args,  # Adiciona a configuração de retries
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        # Estratégia para evitar execuções indesejadas (somente uma ativa por vez)
        max_active_runs=1,
        params={
            'start_date': Param('2025-11-03', type='string'),
            'end_date': Param('2025-11-05', type='string'),
        },
) as dag:
    setup_dirs = ensure_local_data_exists()
    load_meta = load_metadata_local.override(task_id='load_metadata')()

    # 1. LOAD: Retorna a lista de caminhos
    load_csv = load_csv_local()

    # 2. MERGE: Recebe a lista de caminhos de load_csv
    merge = merge_csv(load_csv)

    # 3. TRANSFORM: Recebe o caminho do CSV mergeado
    transform = transform_data(merge)

    # 4. INSERT: Recebe o caminho do CSV final
    insert_task = insert_data_to_sqlite(transform)

    cleanup = cleanup_data()

    # --- FLUXO DE EXECUÇÃO ---

    setup_dirs >> [load_meta, load_csv]

    # O encadeamento TaskFlow (merge(load_csv)) já cria as dependências necessárias,
    # mas garantimos que load_meta também esteja pronto antes da transformação.
    [load_meta, load_csv] >> merge

    merge >> transform >> insert_task >> cleanup
