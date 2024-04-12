import logging
import pandas as pd
import numpy as np
from zenml import step
from sklearn.base import ClassifierMixin
from src.train_model import Training, LogisticRegressionModel, RandomForestModel
from typing import Union

@step
def model_train(X_train : Union[pd.DataFrame , np.ndarray], Y_train : pd.Series) -> ClassifierMixin:

    try : 
        
        model_1 = Training(X_train, Y_train, LogisticRegressionModel())
        model_1_trained = model_1.training()
        logging.info('Training finished')

        return model_1_trained
    
    except Exception as e:
        logging.error('Error while training the model {}'.format(e))




