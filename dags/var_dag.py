from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.sdk.definitions.decorators import task

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

with DAG(
    dag_id = 'var_dag',
    schedule = None,
    start_date = datetime(2020, 1, 1),
) as dag:

    @task
    def ler_mensagens():
        msg = Variable.get('MSG_TESTE')
        print(msg)

    @task
    def ler_json():
        json_data = Variable.get(key="JSON_TESTE", deserialize_json=True)
        print(f"JSON lido: {json_data['campo1']} {json_data['campo2']}")

    ler_mensagens() >> ler_json()
