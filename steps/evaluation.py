import logging 
import pandas as pd
from zenml import step
from sklearn.base import RegressorMixin
from src.model_evaluation import RMSE_Evaluation, Evaluation
import mlflow
from zenml.client import Client

experiment_tracker = Client().active_stack.experiment_tracker


@step(experiment_tracker=experiment_tracker.name)
def evaluate(Y_test : pd.Series, X_test : pd.DataFrame ,model : RegressorMixin) -> float:
    try:
        regression_evaluation = Evaluation(Y_test, X_test , model, RMSE_Evaluation())
        rmse_score = regression_evaluation.evaluation_calcul()
        mlflow.log_metric('RMSE',rmse_score)
        logging.info('Evaluation finished')
        return rmse_score
    
    except Exception as e:
        logging.error(f'Error while calculating evaluation : {e}')
        raise e