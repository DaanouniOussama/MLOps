import logging
import pandas as pd
from typing import Union
from sklearn.model_selection import train_test_split
from abc import ABC, abstractmethod
from sklearn.preprocessing import LabelEncoder

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')
# The abstract class that works as blueprint it contains our abstractmethod
class Strategy(ABC):
    @abstractmethod
    def handle_data(self,data:pd.DataFrame) -> Union[pd.DataFrame, pd.Series]:
        pass

# Definig first strategy as cleaning data 
class Cleaning(Strategy):

    def handle_data(self, data : pd.DataFrame) -> pd.DataFrame:
        try:
            logging.info('Feature engineering column neighbourhood')
            data['neighbourhood_'] = data['neighbourhood'] + ', ' + data['city']
            data = data[['superficie','rooms','bath_room','floor','age','neighbourhood_','city','price']]
            logging.info('Deleting outliers')
            lower_quantile = data.groupby('neighbourhood_')['price'].quantile(0.05).reset_index()
            upper_quantile = data.groupby('neighbourhood_')['price'].quantile(0.95).reset_index()
            lower_quantile.rename(columns={'price': '0.05_price'}, inplace=True)
            upper_quantile.rename(columns={'price': '0.95_price'}, inplace=True)
            adding_2 = pd.merge(data , lower_quantile, on='neighbourhood_', how='left')
            final_df = pd.merge(adding_2 , upper_quantile, on='neighbourhood_', how='left')
            final_df = final_df[(final_df['price']>final_df['0.05_price']) & (final_df['price']<final_df['0.95_price'])]
            final_df = final_df.iloc[:,:-2]
            return final_df
        
        except Exception as e:
            logging.error(f'Error while cleaning data : {e}')
            raise e
        
# Second strategy as encoding variables
class Encoding(Strategy):

    def handle_data(self, data : pd.DataFrame) -> pd.DataFrame:
        try:
            logging.info('Coding cities')
            data.loc[data['city']=='casablanca','city'] = 5
            data.loc[data['city']=='tanger','city'] = 4
            data.loc[data['city']=='marrakech','city'] = 3
            data.loc[data['city']=='agadir','city'] = 2
            data.loc[data['city']=='rabat','city'] = 1
            logging.info('Coding ages')
            data.loc[data['age']=='New','age'] = 0
            data.loc[data['age']=='1-5 years','age'] = 1
            data.loc[data['age']=='6-10 years','age'] = 2
            data.loc[data['age']=='11-21 years','age'] = 3
            data.loc[data['age']=='21+ years','age'] = 4
            logging.info('Label coding the neighboorhoods')
            label_encoder = LabelEncoder()
            # Fit and transform the neighborhood column
            data['neighbourhood_'] = label_encoder.fit_transform(data['neighbourhood_'])

            return data
        except Exception as e:
            logging.error(f'Error while coding variables : {e}')
            raise e

# Third strategy as spliting data into train and test
class Spliting_Data():
    def handle_data(self,data : pd.DataFrame) ->  Union[pd.DataFrame , pd.Series]:
        try:
            logging.info('Spliting data into train and test')
            x_train, x_test, y_train, y_test = train_test_split(data.iloc[:,:-1],
                                                    data.iloc[:,-1],
                                                    test_size = 0.2,
                                                    shuffle = True,   
                                                    random_state = 123)

            return x_train, x_test, y_train, y_test
        
        except Exception as e:
            logging.error(f'Error while spliting data : {e}')
            raise e
        

# The Main class
class PreProcessing:

    def __init__(self, data, strategy):
        self.data = data
        self.strategy = strategy

    def preprocess(self) -> Union[pd.DataFrame, pd.Series] :   
        try:
            return self.strategy.handle_data(self.data)
        
        except Exception as e:
            logging.error(f'error while handling data : {e}')
            raise e
