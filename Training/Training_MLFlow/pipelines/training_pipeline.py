from steps.ingest_data import import_df
from steps.clean_data import clean_df
from steps.model_train import model_train
from steps.evaluation import evaluate
import mlflow
import os


os.environ['MLFLOW_TRACKING_USERNAME'] = 'DaanouniOussama'
os.environ['MLFLOW_TRACKING_PASSWORD'] = '70f0d514b2b8dc0829ed8030f38eb9421734bbcc'

remote_server_uri = "https://dagshub.com/DaanouniOussama/MLOps.mlflow"


def train_pipeline(algo):
    df = import_df('172.19.0.5', 'Real_estate', 'airflow', 'airflow', '5432')
    X_train, X_test, Y_train, Y_test = clean_df(df)
    print(X_train)
    print(X_test)
    mlflow.set_tracking_uri(remote_server_uri)
    mlflow.set_experiment(f'Experiment : {algo}')
    with mlflow.start_run():
        model = model_train(X_train, Y_train,algo)
        rmse = evaluate(Y_test , X_test , model) 