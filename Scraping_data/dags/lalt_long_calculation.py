import logging
import psycopg2
import pandas as pd
import geopy
from geopy.geocoders import Nominatim

def lati_long()->int:
    try:
        logging.info('Connecting to postgres DB ...')
        connection = psycopg2.connect(database="test_DB", user="airflow", password="airflow", host="172.17.111.99", port=5432)
        query = """ SELECT * FROM real_estate_table;"""
        df = pd.read_sql_query(query, connection)
        logging.info('Connection to postgres db was successful')

    except Exception as e:
        logging.error('error while connecting and extracting data from Postgres DB')
        raise e

    try:
        
        df['adress'] = df['neighbourhood'] + ', ' + df['city']
        logging.info('Creating variable Adresse as neighbourhood + city')
        geolocator = Nominatim(user_agent="Real-estate Monitor")
        maps_data = pd.DataFrame()
        maps_data['adresses'] = df['adress'].unique()
        maps_data['location'] = maps_data['adresses'].apply(lambda x: geolocator.geocode(x,language = 'fr'))
        maps_data['latitude'] = maps_data['location'].apply(lambda x: x.latitude if x else None)
        maps_data['longitude'] = maps_data['location'].apply(lambda x: x.longitude if x else None)
        logging.info('Calculating longitude and laltitude')

        maps_data.to_csv('/opt/airflow/dags/long_lalt.csv',index=False)
        logging.info('Saving dataframe into csv')
        return 0

    except Exception as e:
        logging.error(f'Error while connecting and calculation of laltitude and longitude : {e}')
        raise e

