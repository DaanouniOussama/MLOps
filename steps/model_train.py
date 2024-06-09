import logging
import pandas as pd
from zenml import step
from sklearn.base import RegressorMixin
from src.train_model import Training, SupportVectorRegressionModel, RandomForestModel
import mlflow
from zenml.client import Client

experiment_tracker = Client().active_stack.experiment_tracker

@step(experiment_tracker=experiment_tracker.name)
def model_train(X_train : pd.DataFrame , Y_train : pd.Series) -> RegressorMixin:

    try : 
        mlflow.sklearn.autolog()
        model_1 = Training(X_train, Y_train, RandomForestModel())
        model_1_trained = model_1.training()
        mlflow.sklearn.log_model(model_1_trained,'RF_model')
        logging.info('Training finished')

        return model_1_trained
    
    except Exception as e:
        logging.error(f'Error while training the model {e}')




