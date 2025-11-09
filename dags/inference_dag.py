import os
import shutil
import sys
from datetime import datetime
from airflow import DAG
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

from plugins.inference_model import load_test_data, predict, load_model

# --- VARIÁVEIS DE AMBIENTE E CAMINHOS ---
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
ML_DIR = os.path.join(DATA_DIR, 'ml')
INPUT_DIR = os.path.join(ML_DIR, 'input')
OUTPUT_DIR = os.path.join(ML_DIR, 'output')
TRAINING_DIR = os.path.join(ML_DIR, 'training')


# --- FUNÇÕES DAS TASKS ---

@task
def ensure_local_data_exists():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TRAINING_DIR, exist_ok=True)
    print("Diretórios de trabalho locais garantidos.")


@task
def load_data():
    file_name = 'test.csv'
    file_path = os.path.join(INPUT_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"AVISO: Arquivo de teste não encontrado - {file_path}. Parando.")
        # Se os dados de teste são cruciais, devemos levantar uma exceção
        raise FileNotFoundError(f"Arquivo de teste obrigatório não encontrado: {file_path}")
    else:
        print(f"Arquivo CSV encontrado: {file_path}")
        # Retorna o caminho ABSOLUTO para a task de processamento
        return file_path


@task
def load_model_task():
    model_name = 'model.pkl'
    model_path = os.path.join(OUTPUT_DIR, model_name)

    if not os.path.exists(model_path):
        print(f"ERRO: Arquivo do modelo não encontrado - {model_path}. O treinamento precisa ser executado!")
        raise FileNotFoundError(f"Modelo {model_name} não encontrado. Execute a DAG de treinamento primeiro.")
    else:
        print(f"Modelo PKL encontrado: {model_path}")
        # Retorna o caminho ABSOLUTO para a próxima task
        return model_path


@task
def run_inference_model(test_data_path, model_path):
    # 1. Carrega o objeto DataFrame (USANDO O MÓDULO IMPORTADO)
    test_data = load_test_data(test_data_path)

    # 2. Carrega o objeto Modelo PKL (USANDO O MÓDULO IMPORTADO)
    model = load_model(model_path)

    # 3. Executa a predição e salva
    output_path = OUTPUT_DIR

    # O seu módulo inference_model.py contém a função 'predict'
    predict(model, test_data, output_path)

    print(f"Predições salvas em: {os.path.join(output_path, 'predictions.csv')}")
    return os.path.join(output_path, 'predictions.csv')


@task(trigger_rule='all_done')
def cleanup():
    """Limpa APENAS o diretório de dados de INPUT temporários."""

    if os.path.exists(INPUT_DIR):
        for filename in os.listdir(INPUT_DIR):
            file_path = os.path.join(INPUT_DIR, filename)
            # Verifica se o arquivo não é a pasta pai antes de tentar apagar
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        print(f"Arquivos temporários de input em ({INPUT_DIR}) limpos.")

    # Garante que o diretório OUTPUT (onde está o modelo) existe, mas não o limpa.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Limpeza concluída. Modelo e predições (no OUTPUT_DIR) PRESERVADOS.")


with DAG(
        dag_id='model_inference_pipeline',
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    # 1. Setup
    setup = ensure_local_data_exists()

    # 2. Tasks de Carga (Rodam em paralelo)
    data_path = load_data()  # Retorna o caminho para 'test.csv'
    model_path = load_model_task()  # Retorna o caminho para 'model.pkl'

    # 3. Inferência: Recebe os caminhos de forma limpa via TaskFlow
    inference = run_inference_model(
        test_data_path=data_path,
        model_path=model_path
    )

    # 4. Cleanup
    cleanup_task = cleanup()

    # --- FLUXO DE EXECUÇÃO ---

    # Setup deve ocorrer primeiro
    setup >> [data_path, model_path]

    # Inferência só ocorre quando o modelo E os dados estiverem prontos
    [data_path, model_path] >> inference

    # Cleanup é a última tarefa
    inference >> cleanup_task
