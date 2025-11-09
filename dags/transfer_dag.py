import sqlite3
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.python import PythonOperator

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

# Transferir dados de uma tabela de banco de dados para outra tabela e depois visualizar o resultado

# --- Variáveis de Configuração ---
SQLITE_CONN_ID = "meu_sqlite_local"
SOURCE_TABLE = "vendas"
DESTINATION_TABLE = "tabela_produtos"

# Lista de comandos SQL para a task de ETL
# (Esta é a função de transferência de dados/criação de tabela)
CREATE_AND_INSERT_SQL = [
    # 1. DROP (para simular o 'replace' e limpar a execução anterior)
    f"DROP TABLE IF EXISTS {DESTINATION_TABLE};",

    # 2. CREATE TABLE
    f"""
    CREATE TABLE {DESTINATION_TABLE} (
        id INTEGER PRIMARY KEY,
        produto TEXT NOT NULL,
        quantidade INTEGER,
        preco REAL
    );
    """,

    # 3. INSERT INTO... SELECT (a transferência de dados)
    f"""
    INSERT INTO {DESTINATION_TABLE} (produto, quantidade, preco)
    SELECT
        produto,
        quantidade,
        preco
    FROM {SOURCE_TABLE};
    """
]


# --- Função Python para Visualizar (Mantida) ---

def show_data(conn_id: str, table_name: str, **kwargs):
    hook = SqliteHook(sqlite_conn_id=conn_id)
    conn_object = hook.get_connection(conn_id)

    # 1. TENTA LER O CAMINHO DO CAMPO 'Host' (O MAIS PROVÁVEL NO SEU CASO)
    #    O valor do campo Host da UI vai para o atributo .host do objeto Connection.
    db_path = conn_object.host

    # 2. SE O CAMPO HOST ESTIVER VAZIO, TENTA .database
    if not db_path:
        db_path = conn_object.database

    # 3. SE AINDA ASSIM NÃO FUNCIONAR, TENTA LIMPAR A URI (menos confiável, mas seguro como fallback)
    if not db_path:
        try:
            uri = hook.get_uri()
            # Limpa o prefixo 'sqlite:///' ou 'sqlite:////'
            db_path = uri.replace('sqlite:///', '').replace('sqlite:////', '/')
        except Exception:
            db_path = None

    if not db_path:
        raise ValueError(
            f"Não foi possível determinar o caminho do banco de dados para conn_id: {conn_id}. Verifique a configuração Host/Schema/Extra.")

    print(f"DEBUG - Caminho do DB lido: {db_path}")

    conn = None
    try:
        # 4. Conecta com o caminho limpo
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"\n--- Dados da Tabela '{table_name}' ---")
        cursor.execute(f"SELECT * FROM {table_name}")

        column_names = [description[0] for description in cursor.description]
        print(column_names)

        for row in cursor.fetchall():
            print(row)

    except sqlite3.Error as e:
        print(f"ERRO SQL ao ler dados: {e}")
        raise
    finally:
        if conn:
            conn.close()
        print("Conexão com o banco de dados fechada.")


with DAG(
        dag_id="transfer_dag",
        start_date=datetime(2023, 1, 1),
        catchup=False,
        schedule=None,
        tags=["sqlite", "execute_sql"],
) as dag:
    create_and_insert_task = SQLExecuteQueryOperator(
        task_id="create_table_and_insert_data",
        # Use 'conn_id' para o operador genérico
        conn_id=SQLITE_CONN_ID,
        # A lista de comandos SQL é passada para 'sql'
        sql=CREATE_AND_INSERT_SQL,
    )

    show_data_task = PythonOperator(
        task_id="visualizar_dados_destino",
        python_callable=show_data,
        op_kwargs={
            "conn_id": SQLITE_CONN_ID,
            "table_name": DESTINATION_TABLE,
        },
    )

    create_and_insert_task >> show_data_task
