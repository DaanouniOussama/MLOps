import logging 
import pandas as pd
import numpy as np
from zenml import step
from sklearn.base import ClassifierMixin
from src.model_evaluation import Evaluation, ClassificationEvaluation
import mlflow
from zenml.client import Client

experiment_tracker = Client().active_stack.experiment_tracker


@step(experiment_tracker=experiment_tracker.name)
def evaluate(X_test_scaled : np.ndarray, Y_test : np.ndarray, model : ClassifierMixin) -> tuple[float,float,float,float]:
    try:
        Y_pred = model.predict(X_test_scaled)
        classification_evaluation = Evaluation(Y_test, Y_pred, ClassificationEvaluation())
        accuracy_Score, precision_Score, recall_Score, f1_Score = classification_evaluation.evaluation_calcul()
        mlflow.log_metric('Accuracy',accuracy_Score)
        mlflow.log_metric('Precision',precision_Score)
        mlflow.log_metric('Recall',recall_Score)
        mlflow.log_metric('F1_score',f1_Score)
        logging.info('Evaluation finished')
        return (accuracy_Score, precision_Score, recall_Score, f1_Score)
    
    except Exception as e:
        logging.error(f'Error while calculating evaluation : {e}')
        raise e