import logging
import pandas as pd
from abc import ABC, abstractmethod
import math
from sklearn.metrics import mean_squared_error
from sklearn.base import RegressorMixin

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

class StrategyEvaluation(ABC):

    @abstractmethod
    def evaluate(self,  Y_test : pd.Series, X_test : pd.DataFrame, model : RegressorMixin) -> float:

        pass


class RMSE_Evaluation(StrategyEvaluation):

    def evaluate(self,  Y_test : pd.Series, X_test : pd.DataFrame, model : RegressorMixin) -> float:
        try:
            Y_pred = model.predict(X_test)
            mse = mean_squared_error(Y_test, Y_pred)
            rmse = math.sqrt(mse)
            logging.info('Evaluation finished')
            return rmse
        except Exception as e:
            logging.error(f'Error while calculating evaluations : {e}')
            raise e

class Evaluation:

    def __init__(self, Y_test : pd.Series, X_test : pd.DataFrame, model : RegressorMixin, typeevaluation : StrategyEvaluation):
        self.Y_test = Y_test
        self.X_test = X_test
        self.model = model
        self.typeevaluation = typeevaluation

    def evaluation_calcul(self):

        return self.typeevaluation.evaluate(self.Y_test, self.X_test, self.model)
