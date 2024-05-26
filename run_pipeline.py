from pipelines.training_pipeline import train_pipeline
import mlflow
from zenml.client import Client



if __name__ == "__main__":
    print(Client().active_stack.experiment_tracker.get_tracking_uri())
    train_pipeline(data_path = '/home/oussama/MLOps/data/iris.csv')

