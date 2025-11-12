import os
import sys
from datetime import datetime
from airflow import DAG
import json
import shutil

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
HISTORY_DIR = os.path.join(ML_DIR, 'history')
PRODUCTION_DIR = os.path.join(ML_DIR, 'production')
# ID de conexão do banco de dados no Airflow UI para salvar as métricas
DB_CONN_ID = "sqlite_metrics_db"

# --- FUNÇÕES DAS TASKS ---

@task
def ensure_local_data_exists():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TRAINING_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    os.makedirs(PRODUCTION_DIR, exist_ok=True)
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
    output_path = os.path.join(HISTORY_DIR, f'metrics_{file_name}.json')

    # Garante que o diretório exista antes de escrever
    os.makedirs(HISTORY_DIR, exist_ok=True)

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


@task.branch(task_id='compare_last_metric')
def compare_last_metric():
    """
    Compara a acurácia do modelo recém-treinado com o modelo anterior para decidir
    se deve fazer o deploy ou pular.
    """
    hook = SqliteHook(sqlite_conn_id=DB_CONN_ID)

    # Busca as duas execuções mais recentes ordenadas por timestamp
    sql_query = """
                SELECT accuracy
                FROM model_metrics
                ORDER BY run_timestamp DESC LIMIT 2; \
                """

    # O hook retorna uma lista de tuplas: [(acurácia_recente,), (acurácia_antiga,)]
    data = hook.get_records(sql=sql_query)

    # Verifica se há dados suficientes para comparação
    if len(data) < 2:
        print("Aviso: Menos de 2 modelos no histórico. Promovendo por precaução.")
        # Se for o primeiro treino, ou se não houver comparação, geralmente promove-se
        return "deploy_new_model"

    # Extrai a acurácia (está na posição [0] de cada tupla)
    acuracia_nova = data[0][0]  # Primeiro registro (mais recente), primeiro campo (accuracy)
    acuracia_antiga = data[1][0]  # Segundo registro (anterior), primeiro campo (accuracy)

    print(f"Acurácia Atual: {acuracia_nova:.4f}")
    print(f"Acurácia Anterior: {acuracia_antiga:.4f}")

    # Lógica de Decisão: Acurácia nova deve ser MAIOR que a antiga
    if acuracia_nova > acuracia_antiga:
        print("Decisão: PROMOVER. Acurácia nova é superior.")
        return "deploy_new_model"  # Retorna o task_id do deploy
    else:
        print("Decisão: REJEITAR. Acurácia não melhorou ou é igual.")
        return "skip_deployment"  # Retorna o task_id para pular o deploy


@task(task_id='deploy_new_model')
def deploy_model(metrics_dict: dict):  # Recebe o dicionário de métricas do XCom
    """Lógica de deploy: move o modelo treinado para produção."""
    # 🛑 1. VERIFICAÇÃO CRÍTICA DE SAFETY CHECK 🛑
    if metrics_dict is None:
        print("Deploy Pulado: A tarefa de treinamento não produziu métricas válidas.")
        # Se você quer garantir que o fluxo vá para o cleanup, você pode retornar sem exceção.
        return None

    # 1. Obtém o nome dinâmico que foi usado para salvar o arquivo
    dynamic_model_name = metrics_dict.get('model_name')

    # 2. Constrói o nome do arquivo PKL COMPLETO
    model_filename = f'{dynamic_model_name}.pkl'

    # 3. Define os caminhos
    # ORIGEM: Onde o modelo foi salvo pela task 'training_model'
    source_path = os.path.join(OUTPUT_DIR, model_filename)
    # DESTINO: Onde a DAG de Inferência irá procurar (nome fixo)
    dest_path = os.path.join(PRODUCTION_DIR, 'model.pkl')

    # Garante que o diretório de produção exista
    os.makedirs(PRODUCTION_DIR, exist_ok=True)

    # 4. Verificar e MOVER o arquivo
    if os.path.exists(source_path):
        # Remove qualquer modelo antigo (model.pkl) na pasta de produção
        if os.path.exists(dest_path):
            os.unlink(dest_path)

            # Move o arquivo dinâmico (RandomForest-...) para o nome fixo (model.pkl)
        shutil.move(source_path, dest_path)
        print(f"NOVO MODELO ({model_filename}) PROMOVIDO para {dest_path}.")
    else:
        # Se falhar, mostra o caminho exato que falhou (para debugging futuro)
        print(f"ERRO: Arquivo fonte não encontrado em: {source_path}")
        raise FileNotFoundError(f"Arquivo do modelo não encontrado em: {source_path}")


@task(task_id='skip_deployment')
def skip_deploy(metrics_dict: dict):
    """Lógica de pular: o modelo antigo permanece em produção."""
    # 🛑 1. VERIFICAÇÃO CRÍTICA DE SAFETY CHECK 🛑
    if metrics_dict is None:
        print("Deploy Pulado: A tarefa de treinamento não produziu métricas válidas.")
        # Se você quer garantir que o fluxo vá para o cleanup, você pode retornar sem exceção.
        return None
    print("MODELO REJEITADO. MANTER MODELO ATUAL EM PRODUÇÃO.")


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
    os.makedirs(HISTORY_DIR, exist_ok=True)
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
    metrics = training_model(data_path)

    # 4. Save Metrics (JSON e DB)
    save_log_json = save_metrics(metrics)
    save_log_db = log_metrics_to_db(metrics)  # Esta deve rodar antes da decisão

    # 5. Decisão de Retreino e Ramificação
    decisao = compare_last_metric()

    # 6. Crie as instâncias das tarefas de deploy e skip **
    deploy_task_instance = deploy_model(metrics)
    skip_task_instance = skip_deploy(metrics)

    # 7. Cleanup
    cleanup_task = cleanup()

    # --- FLUXO DE EXECUÇÃO CORRIGIDO ---

    # 1. Setup e Treino
    setup >> data_path >> metrics

    # 2. Após o treino, salve os logs em paralelo
    metrics >> [save_log_json, save_log_db]

    # 3. A DECISÃO deve esperar o LOG NO BANCO DE DADOS terminar.
    save_log_db >> decisao

    # 4. Ramificação (Deploy ou Skip)
    decisao >> [deploy_task_instance, skip_task_instance]

    # 5. Finalização
    # cleanup_task só roda DEPOIS do deploy ou do skip.
    [deploy_task_instance, skip_task_instance] >> cleanup_task
