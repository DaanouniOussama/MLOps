import logging 
import pandas as pd
from zenml import step
from sklearn.base import ClassifierMixin
from src.model_evaluation import Evaluation, ClassificationEvaluation

@step
def evaluate(X_test_scaled : pd.DataFrame, Y_test : pd.Series, model : ClassifierMixin):
    try:
        Y_pred = model.predict(X_test_scaled)
        classification_evaluation = Evaluation(Y_test, Y_pred, ClassificationEvaluation())
        accuracy_Score, precision_Score, recall_Score, f1_Score = classification_evaluation.evaluation_calcul()
        logging.info('Evaluation finished')
        return accuracy_Score, precision_Score, recall_Score, f1_Score
    
    except Exception as e:
        logging.error('Error while calculating evaluation : {}'.format(e))
        raise e