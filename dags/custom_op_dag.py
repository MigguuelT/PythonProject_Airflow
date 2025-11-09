import sys
from datetime import datetime
from airflow import DAG
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

from plugins.mask_csv_operator import MaskCsvOperator


with DAG(
    dag_id = 'custom_op_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    @task
    def gerar_arquivo_csv():
        path = AIRFLOW_HOME + '/data/custom_op.csv'
        with open(path, 'w') as f:
            f.write('Miguel, mt, 47, 80, true\n')
            f.write('Dani, dp, 47, 60, true\n')
            f.write('Pri, pp, 35, 65, true\n')
            f.write('Lea, lt, 14, 55, true\n')
            f.write('Maira, mg, 32, 70, true\n')
        # Retorna o caminho do arquivo criado para a próxima task
        return path

    mask_file = MaskCsvOperator(
        task_id = 'mask_file',
        input_file = AIRFLOW_HOME + '/data/custom_op.csv',
        output_file = AIRFLOW_HOME + '/data/custom_op_mask.csv',
        separator=',',
        column= 2
    )

    # O Airflow 2.x trata o retorno da função decorada como a Task pronta:
    task_gerada = gerar_arquivo_csv()

    task_gerada >> mask_file
