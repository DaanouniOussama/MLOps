import logging 
import pandas as pd
import numpy as np
from zenml import step
from src.data_cleaning import DataSplitingStrategy, DataScalingStrategy, PreProcessing
from typing_extensions import Annotated
from typing import Tuple

@step
def clean_df(df: pd.DataFrame)-> Tuple[Annotated[pd.DataFrame,'X_train'],
                                       Annotated[pd.DataFrame,"X_test"],
                                       Annotated[np.ndarray,"Y_train"],
                                       Annotated[np.ndarray,"Y_test"],
                                       Annotated[np.ndarray, "X_train_scaled"],
                                       Annotated[np.ndarray, "X_test_scaled"]
                                       ] :
    try: 
        preprocess_1 = PreProcessing(df , DataSplitingStrategy())
        X_train, X_test, Y_train, Y_test = preprocess_1.preprocess()
    
        preprocess_2 = PreProcessing(X_train, DataScalingStrategy())
        X_train_scaled = preprocess_2.preprocess()
        preprocess_3 = PreProcessing(X_test, DataScalingStrategy())
        X_test_scaled = preprocess_3.preprocess()       
        logging.info('Preprocessong finished')

        return X_train, X_test, Y_train, Y_test, X_train_scaled, X_test_scaled

    except Exception as e:
        logging.error('Error while preprocessing{}'.format(e))
        raise e 
    






    

