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
        preprocess_1 = PreProcessing(df , Cleaning())
        df_cleaned = preprocess_1.preprocess()
    
        preprocess_2 = PreProcessing(df_cleaned, Encoding())
        df_cleaned_encoded = preprocess_2.preprocess()

        # Create Feature store
        # Connecting to db
        #'172.18.0.3', 'Real_estate', 'airflow', 'airflow', '5432'
        conn = psycopg2.connect(dbname = 'Feature_Store' , user = 'airflow', password = 'airflow', host = '172.18.0.3', port = '5432')
        cur = conn.cursor()

        # Create table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS feature_store (
            id SERIAL PRIMARY KEY,
            superficie INT unsigned,
            rooms INT unsigned,
            bath_room INT unsigned,
            floor INT unsigned,
            age INT unsigned,
            neighbourhood_ INT unsigned,
            city INT unsigned,
            price FLOAT unsigned,
        )
        '''

        cur.execute(create_table_query)
        conn.commit()

        # Bulk insert using StringIO
        buffer = StringIO()
        df_cleaned_encoded.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_from(buffer, 'feature_store', sep=",", columns=('superficie', 'rooms', 'bath_room','floor', 'age', 'neighbourhood_', 'city', 'price'))
        # close cur and connection
        # Close the connection
        cur.close()
        conn.close()



        preprocess_3 = PreProcessing(df_cleaned_encoded, Spliting_Data())
        X_train, X_test, Y_train, Y_test = preprocess_3.preprocess()       
        logging.info('Preprocessong finished')

        return X_train, X_test, Y_train, Y_test

    except Exception as e:
        logging.error(f'Error while preprocessing : {e}')
        raise e
    






    

