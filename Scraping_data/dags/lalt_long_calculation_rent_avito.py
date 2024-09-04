import logging
import psycopg2
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def lati_long_rent()->int:
    try:
        logging.info('Loading scraped data ...')
        df = pd.read_csv('/opt/airflow/dags/scraped_data_rent_avito.csv',index_col=0)
        logging.info('Data was loaded successfully')

    except Exception as e:
        logging.error('error while loading data')
        raise e

    try:
        
        df['adress'] = df['secteur'] + ', ' + df['ville']
        logging.info('Creating variable Adresse as neighbourhood + city')
        geolocator = Nominatim(user_agent="Real-estate Monitor")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        maps_data = pd.DataFrame()
        maps_data['adresses'] = df['adress'].unique()
        maps_data['location'] = maps_data['adresses'].apply(lambda x: geocode(x,language = 'fr'))
        maps_data['latitude'] = maps_data['location'].apply(lambda x: x.latitude if x else None)
        maps_data['longitude'] = maps_data['location'].apply(lambda x: x.longitude if x else None)

        maps_data = maps_data[['adresses','latitude','longitude']]
        logging.info('Calculating longitude and laltitude')

        maps_data.to_csv('/opt/airflow/dags/long_lalt_rent_avito.csv',index=False, sep=';')
        logging.info('Saving dataframe into csv')
        return 0

    except Exception as e:
        logging.error(f'Error while connecting and calculation of laltitude and longitude : {e}')
        raise e