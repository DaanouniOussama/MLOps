import psycopg2
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

class IngestData:
    def __init__(self, host: str, dbname : str, user : str, password : str, port : str):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port

    def connextion(self):
        logging.info(f"Connecting to postgres db")
        try:
            conn = psycopg2.connect(host=self.host, dbname= self.dbname,user = self.user, password = self.password , port = self.port)
            query = """ SELECT * FROM feature_store;"""
            # Load data into a pandas DataFrame
            df = pd.read_sql_query(query, conn)
            logging.info("Data loaded successfully into DataFrame")
            return df.iloc[:,1:]
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
        finally:
            if conn:
                conn.close()
                logging.info("Database connection closed")
