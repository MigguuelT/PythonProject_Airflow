import os
import sys
from datetime import datetime
from airflow import DAG
import json

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

from plugins.training_model import load_training_data, train_model

# --- VARIÁVEIS DE AMBIENTE E CAMINHOS ---
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
ML_DIR = os.path.join(DATA_DIR, 'ml')
INPUT_DIR = os.path.join(ML_DIR, 'input')
OUTPUT_DIR = os.path.join(ML_DIR, 'output')
TRAINING_DIR = os.path.join(ML_DIR, 'training')
# ID de conexão do banco de dados no Airflow UI para salvar as métricas
DB_CONN_ID = "sqlite_metrics_db"

# --- FUNÇÕES DAS TASKS ---

@task
def ensure_local_data_exists():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TRAINING_DIR, exist_ok=True)
    print("Diretórios de trabalho locais garantidos.")


@task
def load_data():
    file_name = 'train.csv'
    file_path = os.path.join(TRAINING_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"AVISO: Arquivo não encontrado - {file_path}.")
        return None
    else:
        print(f"Arquivo CSV encontrado: {file_path}")
        return file_path


# CORREÇÃO: A task training_model agora retorna um dicionário serializável
@task(task_id='train_and_evaluate_model')  # Renomeada para clareza
def training_model(train_file_path):
    if train_file_path is None:
        print("Pulando treinamento: Nenhum arquivo de dados encontrado.")
        return None

    data = load_training_data(train_file_path)
    output_path = OUTPUT_DIR

    # train_model agora retorna um dicionário de métricas serializáveis (strings)
    metrics_dict = train_model(data, output_path)

    return metrics_dict


@task
def save_metrics(metrics: dict):
    """Salva as métricas de treinamento na pasta OUTPUT em formato JSON."""

    if metrics is None:
        print("Nenhuma métrica para salvar. Pulando.")
        return None

    file_name = metrics["model_name"].replace(" ", "_").replace("-", "")
    output_path = os.path.join(OUTPUT_DIR, f'metrics_{file_name}.json')

    # Garante que o diretório exista antes de escrever
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    metrics_json = json.dumps(metrics, indent=4)

    with open(output_path, 'w') as f:
        f.write(metrics_json)

    print(f"Métricas salvas em: {output_path}")
    return output_path


@task
def log_metrics_to_db(metrics: dict, **context):
    """
    Recebe as métricas e salva na tabela SQLite usando o Airflow Hook.
    """
    if metrics is None:
        print("Nenhuma métrica para salvar no DB. Pulando.")
        return None

    # O Hook gerencia a conexão com o banco (definida no Airflow UI)
    hook = SqliteHook(sqlite_conn_id=DB_CONN_ID)

    # 1. Recuperar dados do contexto e das métricas
    # A Airflow TaskFlow API já coloca o retorno da task anterior (metrics) como primeiro argumento
    run_id = context['dag_run'].run_id

    # 2. Preparar o SQL e os dados
    sql_insert = f"""
    INSERT INTO model_metrics (
        run_id, 
        run_timestamp, 
        model_version, 
        model_name, 
        id_model, 
        accuracy
    )
    VALUES (?, ?, ?, ?, ?, ?);
    """

    # Cria a tupla de dados, garantindo a ordem correta para o SQL
    data_to_insert = (
        run_id,
        metrics.get('date_model'),  # Usando o campo formatado como timestamp
        metrics.get('model_version'),
        metrics.get('model_name'),
        metrics.get('id_model'),
        metrics.get('accuracy')
    )

    # 3. Executar a inserção
    hook.run(sql_insert, parameters=data_to_insert)
    print(f"Métricas salvas com sucesso no SQLite para o run_id: {run_id}")

    # Remove a tarefa antiga de salvar em JSON para evitar duplicidade
    # return f"SQLite: {run_id}"


@task(trigger_rule='all_done')
def cleanup():
    """Limpa o diretório de dados de treinamento, mas PRESERVA o modelo treinado (OUTPUT_DIR)."""
    if os.path.exists(TRAINING_DIR):
        for filename in os.listdir(TRAINING_DIR):
            file_path = os.path.join(TRAINING_DIR, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        print(f"Arquivos temporários de treinamento em ({TRAINING_DIR}) limpos.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Limpeza concluída. Modelo treinado foi PRESERVADO em OUTPUT_DIR.")


with DAG(
        dag_id='model_training_pipeline_v2',
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    # 1. Setup
    setup = ensure_local_data_exists()

    # 2. Load Data
    data_path = load_data()

    # 3. Training/Evaluation
    metrics = training_model(data_path)  # Retorna um dicionário

    # 4. Save Metrics (JSON - MANTIDO como backup, mas opcional)
    save_log_json = save_metrics(metrics)

    # 5. Save Metrics to DB (NOVO!)
    save_log_db = log_metrics_to_db(metrics)

    # 6. Cleanup
    cleanup_task = cleanup()

    # --- FLUXO DE EXECUÇÃO ATUALIZADO ---
    setup >> data_path >> metrics

    # Após o treino, as ações de log (JSON e DB) podem rodar em paralelo
    metrics >> [save_log_json, save_log_db] >> cleanup_task
