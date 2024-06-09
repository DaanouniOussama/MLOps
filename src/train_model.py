import logging
import pandas as pd
from abc import ABC, abstractmethod
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.base import RegressorMixin

# The abstract class
class Model(ABC):

    @abstractmethod
    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        pass

# First strategy
class SupportVectorRegressionModel(Model):

    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        try :
            SVM = SVR()
            SVM.fit(X_train,Y_train)
            logging.info('Training Support Vector Regression Model finished')
            return SVM
        
        except Exception as e :
            logging.error(f'Error while training Support Vector Regression Model : {e}')
            raise e

# Second strategy
class RandomForestModel(Model):

    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        try :
            RF = RandomForestRegressor(random_state = 125, max_depth=10, n_estimators = 10, max_features = 3)
            RF.fit(X_train, Y_train)
            logging.info('Training RandomForest Model finished')
            return RF
        
        except Exception as e :
            logging.error(f'Error while training RandomForest Model : {e}')
            raise e

# The main class
class Training:

    def __init__(self, X_train : pd.DataFrame, Y_train : pd.Series, model : Model):

        self.X_train = X_train
        self.Y_train = Y_train
        self.model = model

    def training(self):

        return self.model.train_model(self.X_train, self.Y_train)

