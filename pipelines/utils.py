import logging

import pandas as pd 
from src.data_cleaning import  PreProcessing,DataScalingStrategy


# Function to get data for testing purposes
def get_data_for_test():
    try:
        df = pd.read_csv('C:/Users/DELL XPS/Documents/MLops/MLOps/data/iris.csv')
        df = df.iloc[:,:-1]
        preprocess = PreProcessing(df, DataScalingStrategy())
        df = preprocess.preprocess()
        
        result = df.to_json(orient="split")
        return result
    except Exception as e:
        logging.error("e")
        raise e