import logging
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from typing import Union
from typing_extensions import Literal

class StrategyEvaluation(ABC):

    @abstractmethod
    def evaluate(self,  Y_test , Y_pred ):

        pass


class ClassificationEvaluation(StrategyEvaluation):

    def evaluate(self,  Y_test , Y_pred):
        try:
            accuracyScore = accuracy_score(Y_test, Y_pred)
            precisionScore = precision_score(Y_test, Y_pred,average='weighted')
            recallScore = recall_score(Y_test, Y_pred,average='weighted')
            f1Score = f1_score(Y_test, Y_pred,average='weighted')
            logging.info('Evaluation finished')
            return accuracyScore, precisionScore, recallScore, f1Score
        except Exception as e:
            logging.error('Error while calculating evaluations {}'.format(e))
            raise e

class Evaluation:

    def __init__(self, Y_test, Y_pred, typeevaluation):
        self.Y_test = Y_test
        self.Y_pred = Y_pred
        self.typeevaluation = typeevaluation

    def evaluation_calcul(self):

        return self.typeevaluation.evaluate(self.Y_test, self.Y_pred)


        








