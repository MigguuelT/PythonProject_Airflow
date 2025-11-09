import pickle
import pandas as pd

AIRFLOW_HOME = '/Users/migueltorikachvili/PycharmProjects/PythonProject_Airflow'

def load_test_data(path):
    return pd.read_csv(path, sep=';')


def load_model(model_path):
    return pickle.load(open(model_path, 'rb'))


def predict(model, test_data, output_path):
    features = ['pclass', 'sex', 'sibsp', 'parch']
    data = pd.get_dummies(test_data[features])
    predictions = model.predict(data)

    # CORREÇÃO: Usar o índice do DataFrame como ID sequencial
    output = pd.DataFrame({
        'passengerId': test_data.index,  # Usa o índice sequencial (0, 1, 2, ...)
        'survived': predictions
    })

    output.to_csv(f'{output_path}/predictions.csv', index=False)
