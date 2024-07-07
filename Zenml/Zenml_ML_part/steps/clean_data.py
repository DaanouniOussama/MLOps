import logging 
import pandas as pd
import numpy as np
from zenml import step
from src.data_cleaning import Cleaning, Encoding, Spliting_Data, PreProcessing
from typing_extensions import Annotated
from typing import Tuple

@step
def clean_df(df: pd.DataFrame)-> Tuple[Annotated[pd.DataFrame,'X_train'],
                                       Annotated[pd.DataFrame,"X_test"],
                                       Annotated[pd.Series,"Y_train"],
                                       Annotated[pd.Series,"Y_test"],
                                       ]:
    try: 
        preprocess_1 = PreProcessing(df , Cleaning())
        df_cleaned = preprocess_1.preprocess()
    
        preprocess_2 = PreProcessing(df_cleaned, Encoding())
        df_cleaned_encoded = preprocess_2.preprocess()

        preprocess_3 = PreProcessing(df_cleaned_encoded, Spliting_Data())
        X_train, X_test, Y_train, Y_test = preprocess_3.preprocess()       
        logging.info('Preprocessong finished')

        return X_train, X_test, Y_train, Y_test

    except Exception as e:
        logging.error(f'Error while preprocessing : {e}')
        raise e 
    






    

