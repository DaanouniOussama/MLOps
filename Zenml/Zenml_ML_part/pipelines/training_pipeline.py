from zenml import pipeline
from steps.ingest_data import import_df
from steps.clean_data import clean_df
from steps.model_train import model_train
from steps.evaluation import evaluate
from zenml.config import DockerSettings
from zenml.integrations.mlflow.steps.mlflow_registry import mlflow_register_model_step

docker_settings = DockerSettings()

@pipeline
def train_pipeline(host, dbname, user, password, port):
    df = import_df(host, dbname, user, password, port)
    X_train, X_test, Y_train, Y_test = clean_df(df)
    model = model_train(X_train, Y_train)
    mlflow_register_model_step(
        model=model
    )
    rmse = evaluate(Y_test , X_test , model) 