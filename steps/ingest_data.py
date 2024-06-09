import logging
from zenml import step
from src.conn_ingest import IngestData
import pandas as pd
    

@step
def import_df(host: str, dbname : str, user : str, password : str, port : str) -> pd.DataFrame:

    try :
        ingest_data = IngestData(host, dbname , user, password, port)
        df = ingest_data.connextion()
        return df
    except Exception as e :
        logging.error(f"Error while importing data : {e}")
        raise e

       