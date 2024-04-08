import logging
import pandas as pd
from typing import Union
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from abc import ABC, abstractmethod
import numpy as np

# The abstract class that works as blueprint it contains our abstractmethod
class Strategy(ABC):
    @abstractmethod
    def handle_data(self,data:pd.DataFrame) -> Union[pd.DataFrame, pd.Series, np.ndarray]:
        pass

    
# First algorithm strategy
class DataSplitingStrategy(Strategy):

    def handle_data(self,data : pd.DataFrame) -> Union[pd.DataFrame , pd.Series]:
        try:
            X_train, X_test, Y_train, Y_test = train_test_split(data.iloc[:,:-1], data.iloc[:,-1], test_size=0.2 , 
                                                                shuffle=True, random_state=123
                                                                )
            logging.info('Spliting data finished')
            return  X_train, X_test, Y_train, Y_test
        
        except Exception as e:
            logging.error('Problem while spliting data : {}'.format(e))
            raise e
        
# Second algorithm strategy
class DataScalingStrategy(Strategy):

    def handle_data(self, X : pd.DataFrame)-> np.ndarray:
        logging.info("Start Scaling data  ...")
        try:
            Scaler = StandardScaler()
            X_scaled = Scaler.fit_transform(X)
            logging.info("Scaling data finished")
            return X_scaled
        
        except Exception as e:
            logging.error('Error while scaling data : {}'.format(e))
            raise e
        

# The Main class
class PreProcessing:

    def __init__(self, data, strategy):
        self.data = data
        self.strategy = strategy

    def preprocess(self) -> Union[pd.DataFrame, pd.Series, np.ndarray] :   
        try:
            return self.strategy.handle_data(self.data)
        
        except Exception as e:
            logging.error('error while handling data{}'.format(e))
            raise e
        




