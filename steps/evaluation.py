import logging 
import pandas as pd
from zenml import step

@step
def evaluation(df : pd.DataFrame)-> None:
    pass
