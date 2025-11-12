import os.path
import pickle
import uuid
from datetime import datetime
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'


def load_training_data(path):
    return pd.read_csv(path, sep=';')


def train_model(train_data, model_path):
    y = train_data['survived']

    features = ['pclass', 'sex', 'sibsp', 'parch']
    X = pd.get_dummies(train_data[features])

    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=1)
    model.fit(x_train, y_train)

    accuracy = model.score(x_test, y_test)

    # Geração dos metadados
    date_model = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    model_name = f'RandomForest - {date_model}'
    id_model = str(uuid.uuid4())

    # Exemplo de versão. Adicione sua lógica de versionamento real aqui.
    model_version = "1.0.0"

    if not os.path.isdir(model_path):
        os.makedirs(model_path)

    # Salva o modelo
    pickle.dump(model, open(os.path.join(model_path, f'{model_name}.pkl'), 'wb'))

    # Retorna o dicionário COMPLETO de métricas para a DAG
    return {
        'model_name': model_name,
        'accuracy': accuracy,
        'date_model': date_model,  # Será usado como run_timestamp
        'id_model': id_model,
        'model_version': model_version
    }