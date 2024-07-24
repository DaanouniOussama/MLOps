import logging 
import pandas as pd
from src.data_cleaning import Cleaning, Encoding, Spliting_Data, PreProcessing
from typing_extensions import Annotated
from typing import Tuple
import psycopg2
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

def clean_df(df: pd.DataFrame)-> Tuple[Annotated[pd.DataFrame,'X_train'],
                                       Annotated[pd.DataFrame,"X_test"],
                                       Annotated[pd.Series,"Y_train"],
                                       Annotated[pd.Series,"Y_test"],
                                       ]:
    try: 

        preprocess = PreProcessing(df, Spliting_Data())
        X_train, X_test, Y_train, Y_test = preprocess.preprocess()       
        logging.info('Preprocessong finished')

        return X_train, X_test, Y_train, Y_test

    except Exception as e:
        logging.error(f'Error while preprocessing : {e}')
        raise e
    






    

