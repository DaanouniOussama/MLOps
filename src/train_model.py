import logging
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.base import ClassifierMixin
from typing import Union

# The abstract class
class Model(ABC):

    @abstractmethod
    def train_model(self, X_train : Union[pd.DataFrame,np.ndarray], Y_train : np.ndarray) -> ClassifierMixin:
        pass

# First strategy
class LogisticRegressionModel(Model):

    def train_model(self, X_train : Union[pd.DataFrame,np.ndarray], Y_train : np.ndarray) -> ClassifierMixin:
        try :
            LR = LogisticRegression()
            LR.fit(X_train,Y_train)
            logging.info('Training LogisticRegression Model finished')
            return LR
        
        except Exception as e :
            logging.error(f'Error while training LogisticRegression Model : {e}')
            raise e

# Second strategy
class RandomForestModel(Model):

    def train_model(self, X_train : Union[pd.DataFrame,np.ndarray], Y_train : np.ndarray) -> ClassifierMixin:
        try :
            RF = RandomForestClassifier()
            RF.fit(X_train, Y_train)
            logging.info('Training RandomForest Model finished')
            return RF
        
        except Exception as e :
            logging.error(f'Error while training RandomForest Model : {e}')
            raise e

# The main class
class Training:

    def __init__(self, X_train : Union[pd.DataFrame, np.ndarray], Y_train : np.ndarray, model : Model):

        self.X_train = X_train
        self.Y_train = Y_train
        self.model = model

    def training(self):

        return self.model.train_model(self.X_train, self.Y_train)

