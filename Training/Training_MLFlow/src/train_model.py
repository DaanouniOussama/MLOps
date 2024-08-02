import logging
import pandas as pd
from abc import ABC, abstractmethod
import xgboost as xg
from sklearn.ensemble import RandomForestRegressor
from sklearn.base import RegressorMixin

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

# The abstract class
class Model(ABC):

    @abstractmethod
    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        pass

# First strategy
class XGBoostRegressionModel(Model):

    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        try :
            # Instantiation 
            xgb_r = xg.XGBRegressor(objective ='reg:linear', n_estimators = 10, enable_categorical=True, seed = 123) 
            xgb_r.fit(X_train,Y_train)
            logging.info('Training XGBoost Regression Model finished')
            return xgb_r
        
        except Exception as e :
            logging.error(f'Error while training XGBoost Regression Model : {e}')
            raise e

# Second strategy
class RandomForestModel(Model):

    def train_model(self, X_train : pd.DataFrame, Y_train : pd.Series) -> RegressorMixin:
        try :
            RF = RandomForestRegressor(random_state = 125, max_depth=25, n_estimators = 10, max_features = 6)
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

