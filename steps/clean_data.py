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
                                       Annotated[pd.Series,"Y_train"],
                                       Annotated[pd.Series,"Y_test"],
                                       Annotated[np.ndarray, "X_scaled"]
                                       ] :
    try: 
        preprocess_1 = PreProcessing(df , DataSplitingStrategy())
        X_train, X_test, Y_train, Y_test = preprocess_1.preprocess()
    
        preprocess_2 = PreProcessing(X_train, DataScalingStrategy())
        scaled_data = preprocess_2.preprocess()
        logging.info('Preprocessong finished')

        return X_train, X_test, Y_train, Y_test, scaled_data

    except Exception as e:
        logging.error('Error while preprocessing{}'.format(e))
        raise e 
    






    

