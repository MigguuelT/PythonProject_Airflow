import os
import shutil
from datetime import datetime
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


# --- FUNÇÕES DAS TASKS ---

# Nota: As funções de 'load' foram convertidas em PythonOperators que simulam o download,
# assumindo que os arquivos JASON e CSV JÁ EXISTEM localmente para o teste.

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
    print("Metadados JSON carregados/existentes localmente.")


@task
def load_csv_local(**context):
    """
    Simula o carregamento de CSVs locais e verifica a existência.
    Apenas imprime os nomes dos arquivos esperados.
    """
    start_date = context['params']['start_date']
    end_date = context['params']['end_date']

    print(f"Carregando arquivos CSV de {start_date} a {end_date}...")

    for date in pd.date_range(start_date, end_date):
        file_path = os.path.join(CSV_DIR, f'data_{date.strftime("%Y-%m-%d")}.csv')
        # Para testes locais, o arquivo precisa existir!
        if not os.path.exists(file_path):
            print(f"AVISO: Arquivo não encontrado - {file_path}. Crie-o para testes.")
        else:
            print(f"Arquivo CSV encontrado: {file_path}")


@task
def merge_csv(**context):
    df_list = []
    start_date = context['params']['start_date']
    end_date = context['params']['end_date']

    # 1. RECRIE O CSV_DIR DENTRO DA FUNÇÃO PARA GARANTIR O SCOPE
    # Use os caminhos absolutos definidos nas globais
    global CSV_DIR  # Garante acesso à variável global

    print(f"DEBUG: CSV_DIR LIDO é {CSV_DIR}")  # Verifique se esta string está correta

    for date in pd.date_range(start_date, end_date):
        # 2. Constrói o caminho completo:
        file_path = os.path.join(CSV_DIR, f'data_{date.strftime("%Y-%m-%d")}.csv')

        print(f"DEBUG: Verificando a existência do arquivo: {file_path}")

        if os.path.exists(file_path):
            df_list.append(pd.read_csv(file_path))
        else:
            print(f"AVISO: Não encontrado o arquivo: {file_path}")

    if not df_list:
        # AQUI ESTÁ O ERRO
        raise ValueError("Nenhum arquivo CSV encontrado para mesclar.")

    df = pd.concat(df_list, ignore_index=True)

    output_path = os.path.join(RESULTS_DIR, 'main_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Mesclagem de CSV concluída. Salvo em: {output_path}")


@task
def transform_data():
    main_path = os.path.join(RESULTS_DIR, 'main_data.csv')
    product_path = os.path.join(JSON_DIR, 'products.json')
    store_path = os.path.join(JSON_DIR, 'stores.json')
    salesperson_path = os.path.join(JSON_DIR, 'salesperson.json')

    main_df = pd.read_csv(main_path)
    # Supondo que os arquivos JSON existam para testes:
    product_df = pd.read_json(product_path)
    store_df = pd.read_json(store_path)
    salesperson_df = pd.read_json(salesperson_path)

    # Lógica de transformação (mantida)
    product_df.rename(columns={'id': 'product_id'}, inplace=True)
    salesperson_df.rename(columns={'id': 'salesperson_id'}, inplace=True)

    final_df = main_df.merge(product_df, left_on='product', right_on='product_id', how='left')
    final_df = final_df.merge(store_df, left_on='store', right_on='store_id', how='left')
    final_df = final_df.merge(salesperson_df, left_on='salesperson', right_on='salesperson_id', how='left')

    output_path = os.path.join(RESULTS_DIR, 'final_data.csv')
    final_df.to_csv(output_path, index=False)
    print(f"Transformação concluída. Salvo em: {output_path}")


@task(trigger_rule='all_done')
def cleanup_data():
    """Limpa APENAS os diretórios de saída (results e sql)."""

    # 1. Limpa o diretório de resultados e seus conteúdos
    shutil.rmtree(RESULTS_DIR, ignore_errors=True)

    # Recria a pasta de results (opcional, mas bom para garantir que existe para a próxima execução)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 2. Limpa o diretório de scripts SQL
    shutil.rmtree(SQL_DIR, ignore_errors=True)
    os.makedirs(SQL_DIR, exist_ok=True)

    print(f"Diretórios de saída ({RESULTS_DIR} e {SQL_DIR}) limpos.")


# --- DEFINIÇÃO DA DAG ---

with DAG(
        dag_id='pipeline_etl_local_dag',
        schedule=None,
        start_date=datetime(2020, 1, 1),
        params={
            # CORREÇÃO: Uso de Param importado de airflow.models.param
            'start_date': Param('2025-11-03', type='string'),
            'end_date': Param('2025-11-05', type='string'),
        },
        # template_searchpath = 'data/etl/results.sql' # Removido, pois não é usado
        catchup=False,
) as dag:
    # TAREFAS DE CONFIGURAÇÃO INICIAL
    setup_dirs = ensure_local_data_exists()

    # TAREFAS DE LOAD (SIMULAÇÃO LOCAL)
    load_meta = load_metadata_local.override(task_id='load_metadata')()
    load_csv = load_csv_local()

    # TAREFAS DE PROCESSAMENTO
    merge = merge_csv()
    transform = transform_data()


    @task(task_id='insert_table')
    def insert_data_to_sqlite():
        """Lê o CSV final, extrai os dados e usa hook.insert_rows (executemany) para inserir."""

        df_path = os.path.join(RESULTS_DIR, 'final_data.csv')
        df = pd.read_csv(df_path)

        # Lista de colunas a serem inseridas (deve bater com a ordem da tabela 'sales')
        target_fields = ['id', 'name', 'state', 'store_name', 'store_adress', 'product_name', 'salesperson_id']

        # Converte o DataFrame para o formato de lista de tuplas que o hook espera
        # Usando apenas as colunas necessárias para a inserção
        data_to_insert = df[target_fields].values.tolist()

        # 2. Executa a inserção em massa
        hook = SqliteHook(sqlite_conn_id='sqlite_default')

        hook.insert_rows(
            table='sales',
            rows=data_to_insert,
            target_fields=target_fields,
            replace=True  # Opcional: útil para testes, substitui linhas existentes
        )
        print(f"Dados inseridos no SQLite usando insert_rows. Total de {len(data_to_insert)} linhas.")


    insert_task = insert_data_to_sqlite()

    # TAREFA DE CLEANUP
    cleanup = cleanup_data()

    # --- FLUXO DE EXECUÇÃO ---

    # 1. Configuração de diretórios
    setup_dirs

    # 2. Download e Merge: setup_dirs garante que tudo pode prosseguir
    setup_dirs >> [load_meta, load_csv]

    # 3. Processamento - FLUXO AJUSTADO:
    [load_meta, load_csv] >> merge >> transform >> insert_task >> cleanup
