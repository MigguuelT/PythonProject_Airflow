import sqlite3
import os

# COPIE O CONTEÚDO EXATO DO CAMPO 'HOST' DA SUA CONEXÃO AIRFLOW AQUI!
# Se você usou 4 barras, coloque as 4 barras.
AIRFLOW_CONN_URI = 'sqlite:////Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow/data/meu_banco_dados.db'

# O driver sqlite3 só precisa do caminho do arquivo, não do prefixo 'sqlite:////'
# Precisamos remover o prefixo.
DB_PATH = AIRFLOW_CONN_URI.replace('sqlite:///', '').replace('sqlite:////', '') 

print(f"Tentando abrir o arquivo: {DB_PATH}")

try:
    # Tenta se conectar (e cria se não existir)
    conn = sqlite3.connect(DB_PATH)
    print("✅ Conexão bem-sucedida com o arquivo!")

    # Tenta executar uma query simples para verificar se a tabela existe
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'")
    result = cursor.fetchone()

    if result:
        print("✅ Tabela 'vendas' existe.")
    else:
        print("❌ Tabela 'vendas' NÃO existe. Certifique-se de criá-la.")

    conn.close()

except sqlite3.OperationalError as e:
    print(f"❌ ERRO DE ACESSO DIRETO: {e}")
    print("O problema é definitivamente o caminho ou a permissão para a pasta!")
except Exception as e:
    print(f"❌ Ocorreu um erro inesperado: {e}")