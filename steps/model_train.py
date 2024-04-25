import logging
import pandas as pd
import numpy as np
from zenml import step
from sklearn.base import ClassifierMixin
from src.train_model import Training, LogisticRegressionModel, RandomForestModel
from typing import Union
import mlflow
from zenml.client import Client

experiment_tracker = Client().active_stack.experiment_tracker

@step(experiment_tracker=experiment_tracker.name)
def model_train(X_train : Union[pd.DataFrame , np.ndarray], Y_train : np.ndarray) -> ClassifierMixin:

    try : 
        mlflow.sklearn.autolog()
        model_1 = Training(X_train, Y_train, LogisticRegressionModel())
        model_1_trained = model_1.training()
        mlflow.sklearn.log_model(model_1_trained,'LR_model')
        logging.info('Training finished')

        return model_1_trained
    
    except Exception as e:
        logging.error(f'Error while training the model {e}')




