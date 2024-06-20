import logging
import streamlit as st
import plotly.express as px
import psycopg2
import pandas as pd
import folium
from geopy.geocoders import Nominatim
from folium import plugins



# Connecting and Fetching all rows from database 
try:
    logging.info('Connecting to postgres DB ...')
    connection = psycopg2.connect(database="test_DB", user="airflow", password="airflow", host="172.17.111.99", port=5432)
    query = """ SELECT * FROM real_estate_table;"""
    df = pd.read_sql_query(query, connection)
    logging.info('Connection to postgres db was successful')

except Exception as e:
    logging.error('error while connecting and extracting data from Postgres DB')
    raise e

# Outliers deletion
# price outliers
try:
    df = df[df['price'] <= df['price'].quantile(0.97)]
    df = df[df['superficie'] <= df['superficie'].quantile(0.97)]
    logging.info('Deleting outliers')
except Exception as e:
    logging.error('Error while deleting outliers')
    raise e

df['adress'] = df['neighbourhood'] + ', ' + df['city']



geolocator = Nominatim(user_agent="Real-estate Monitor")
maps_data = pd.DataFrame()
maps_data['adresses'] = df['adress'].unique()
maps_data['location'] = maps_data['adresses'].apply(lambda x: geolocator.geocode(x,language = 'fr'))
maps_data['latitude'] = maps_data['location'].apply(lambda x: x.latitude if x else None)
maps_data['longitude'] = maps_data['location'].apply(lambda x: x.longitude if x else None)

final = pd.merge(df,maps_data,left_on='adress', right_on='adresses')
#df['location'] = df['adress'].apply(lambda x: geolocator.geocode(x))
#df['latitude'] = df['location'].apply(lambda x: x.latitude if x else None)
#df['longitude'] = df['location'].apply(lambda x: x.longitude if x else None)


st.write(final)

#location = geolocator.geocode("")






fig = px.scatter(df, x = 'superficie', y = 'price')
st.plotly_chart(fig)