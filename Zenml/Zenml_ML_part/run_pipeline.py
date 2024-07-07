from pipelines.training_pipeline import train_pipeline
import mlflow
from zenml.client import Client

# database="Real_estate", user="airflow", password="airflow", host="172.18.0.2", port=5432

if __name__ == "__main__":
    print(Client().active_stack.experiment_tracker.get_tracking_uri())
    train_pipeline('localhost', 'Real_estate', 'airflow', 'airflow', '54320')

