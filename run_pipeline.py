from pipelines.training_pipeline import train_pipeline
import mlflow
from zenml.client import Client

# host : '172.17.111.99
# dbname : 'test_DB'
# user : 'airflow'
# password : 'airflow'
# port : '5432'

if __name__ == "__main__":
    print(Client().active_stack.experiment_tracker.get_tracking_uri())
    train_pipeline('localhost', 'test_DB', 'airflow', 'airflow', '5432')

