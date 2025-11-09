import os
import shutil
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.sdk.definitions.decorators import task

# --- VARIÁVEIS DE AMBIENTE E CAMINHOS ---
AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
ETL_DIR = os.path.join(DATA_DIR, 'etl')
RESULTS_DIR = os.path.join(ETL_DIR, 'results')
JSON_DIR = os.path.join(ETL_DIR, 'json')
CSV_DIR = os.path.join(ETL_DIR, 'csv')
SQL_DIR = os.path.join(ETL_DIR, 'sql')


# --- FUNÇÕES DAS TASKS ---

@task
def ensure_local_data_exists():
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(SQL_DIR, exist_ok=True)
    print("Diretórios de trabalho locais garantidos.")


@task
def load_metadata_local():
    """Simula o carregamento de metadados JSON localmente."""
    return [os.path.join(JSON_DIR, 'products.json'), os.path.join(JSON_DIR, 'stores.json'),
            os.path.join(JSON_DIR, 'salesperson.json')]


@task
def load_csv_local(ds=None):  # Recebe a data de execução como string (YYYY-MM-DD)
    """Carrega o arquivo CSV referente à data de execução."""

    if ds is None:
        raise ValueError("A data de execução (ds) é obrigatória para o processamento incremental.")

    file_name = f'data_{ds}.csv'
    file_path = os.path.join(CSV_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"AVISO: Arquivo incremental não encontrado - {file_path}. Pulando...")
        # Se o arquivo não existe, retornamos None ou levantamos erro dependendo da regra de negócio
        return None
    else:
        print(f"Arquivo CSV incremental encontrado: {file_path}")
        # Retorna o caminho do arquivo para a próxima task
        return file_path


@task
def transform_data(main_csv_path):  # Recebe o caminho do CSV retornado por load_csv_local
    """Transforma os dados do arquivo CSV do dia com metadados JSON."""

    if main_csv_path is None:
        print("Nenhum CSV para processar. Finalizando transformação.")
        return  # Finaliza a task sem falhar se load_csv_local retornou None

    # Lógica de leitura de dados (adaptada para usar o caminho recebido)
    main_df = pd.read_csv(main_csv_path)

    # Leituras de JSON
    product_df = pd.read_json(os.path.join(JSON_DIR, 'products.json'))
    store_df = pd.read_json(os.path.join(JSON_DIR, 'stores.json'))
    salesperson_df = pd.read_json(os.path.join(JSON_DIR, 'salesperson.json'))

    # ... (Restante da Lógica de Transformação)
    product_df.rename(columns={'id': 'product_id'}, inplace=True)
    salesperson_df.rename(columns={'id': 'salesperson_id'}, inplace=True)

    final_df = main_df.merge(product_df, left_on='product', right_on='product_id', how='left')
    final_df = final_df.merge(store_df, left_on='store', right_on='store_id', how='left')
    final_df = final_df.merge(salesperson_df, left_on='salesperson', right_on='salesperson_id', how='left')

    output_path = os.path.join(RESULTS_DIR, 'final_data.csv')
    final_df.to_csv(output_path, index=False)
    print(f"Transformação concluída. Salvo em: {output_path}")
    return output_path


@task(trigger_rule='all_done')
def cleanup_data():
    # ... (mantido o mesmo)
    shutil.rmtree(RESULTS_DIR, ignore_errors=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    shutil.rmtree(SQL_DIR, ignore_errors=True)
    os.makedirs(SQL_DIR, exist_ok=True)
    print(f"Diretórios de saída ({RESULTS_DIR} e {SQL_DIR}) limpos.")


@task(task_id='insert_table')
def insert_data_to_sqlite(final_csv_path):  # Recebe o caminho do CSV da task anterior
    """Lê o CSV final, extrai os dados e usa hook.insert_rows (executemany) para inserir."""

    if final_csv_path is None:
        print("Nenhum arquivo final para inserir. Pulando inserção.")
        return  # Finaliza a task

    df = pd.read_csv(final_csv_path)

    # Lista de colunas a serem inseridas (deve bater com a ordem da tabela 'sales')
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


# --- DEFINIÇÃO DA DAG ---

with DAG(
        dag_id='pipeline_etl_incremental_dag',
        schedule='@daily',  # Mudado para execução diária
        start_date=datetime(2025, 11, 1),  # Data de início anterior para testar o catchup/schedule
        catchup=False,
) as dag:
    # TAREFAS DE CONFIGURAÇÃO INICIAL
    setup_dirs = ensure_local_data_exists()

    # TAREFAS DE LOAD E PROCESSAMENTO
    load_meta = load_metadata_local.override(task_id='load_metadata')()

    # load_csv_local é chamado sem argumento, o Airflow injeta 'ds' automaticamente
    load_csv = load_csv_local()

    # Transform recebe o caminho do CSV que load_csv retornou
    transform = transform_data(load_csv)

    # Insert recebe o caminho do CSV transformado
    insert_task = insert_data_to_sqlite(transform)

    # TAREFA DE CLEANUP
    cleanup = cleanup_data()

    # --- FLUXO DE EXECUÇÃO ---

    # Setup de diretórios é o primeiro
    setup_dirs >> load_meta
    setup_dirs >> load_csv  # load_csv não depende de load_meta

    # Transformação e Inserção
    [load_meta, load_csv] >> transform >> insert_task >> cleanup
