import logging
import pandas as pd 
from src.data_cleaning import  PreProcessing,DataScalingStrategy
import numpy as np



# Function to get data for testing purposes
def get_data_for_test():
    try:
        df = pd.read_csv('/home/oussama/MLOps/data/iris.csv')
        df = df.iloc[:,:-1]
        preprocess = PreProcessing(df, DataScalingStrategy())
        df = preprocess.preprocess()

        return df
    except Exception as e:
        logging.error("e")
        raise e