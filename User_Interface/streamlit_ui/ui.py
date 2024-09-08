import logging
import streamlit as st
import plotly.express as px
import psycopg2
import pandas as pd
import pydeck as pdk
import altair as alt
import plotly.graph_objects as go
from dashboard import dashboard
from advanced_analytics import advanced_analytics
from ai import ai


st.set_page_config(
        page_title="Moroccan Real-estate Monitor",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded")

option = st.radio('Select :',
                  ['Avito',
                   'Mubawab'],
                  horizontal=True)
option = option.lower()

# Map the selected option to the corresponding table name
table_name = f"real_estate_table_{option}_vente"

try:
    logging.info('Connecting to postgres DB ...')
    connection = psycopg2.connect(database="Real_estate", user="airflow", password="airflow", host="localhost", port=54320)
    # Dynamically format the query based on the selected option
    query1 = f"SELECT * FROM {table_name};"

    #query2 = """ SELECT neighbourhood_city, neighbourhood_city_coded, city FROM feature_store_appartement;"""
    df = pd.read_sql_query(query1, connection)
    #feature_store = pd.read_sql_query(query2, connection)
    logging.info('Connection to postgres db was successful')

except Exception as e:
    logging.error('error while connecting and extracting data from Postgres DB')
    raise e

# Outliers deletion
# price outliers
# try:
#     df = df[df['price'] <= df['price'].quantile(0.97)]
#     df = df[df['surface_totale'] <= df['surface_totale'].quantile(0.97)]
#     logging.info('Deleting outliers')
# except Exception as e:
#     logging.error('Error while deleting outliers')
#     raise e

df['adress'] = df['secteur'] + ', ' + df['ville']

# Connecting and Fetching all rows from database Maps_data
try:
    #logging.info('Connecting to postgres DB ...')
    #connection = psycopg2.connect(database="Real_estate", user="airflow", password="airflow", host="localhost", port=54320)
    query = """ SELECT * FROM maps_table_avito_sell;"""
    df_maps = pd.read_sql_query(query, connection)
    logging.info('Connection to postgres db was successful')

except Exception as e:
    logging.error('error while connecting and extracting data from Postgres DB')
    raise e


df_maps = df_maps.dropna()

#df_maps = df_maps[['latitude','longitude']]
df_maps = df_maps.loc[(df_maps['laltitude']>21.0) & (df_maps['laltitude']<36.0) & (df_maps['longitude']>-17.0) & (df_maps['longitude']<-1.0)]

df_maps = df_maps.rename(columns={"laltitude": "latitude"})

#st.map(df_maps, zoom=6)

# merge df with df_maps
df_merged = pd.merge(df,df_maps, left_on='adress', right_on='neighbourhood_city')


# Define a function to normalize the prices and map them to colors
def price_to_color(price):
    min_price = df_merged['price'].min()
    max_price = df_merged['price'].max()
    # Normalize price to a range of 0 to 255
    normalized_price = (price - min_price) / (max_price - min_price) * 255
    return [255 - normalized_price, normalized_price, 0, 160]  # RGB with alpha

# Apply the function to the DataFrame
df_merged['color'] = df_merged['price'].apply(price_to_color)


##### ^^^^^^^^^^^ @@@@@@@@@@@@ End Calculation     @@@@@@@@@@@@ ^^^^^^^^^^^ #####

# Create a sidebar for navigation
st.sidebar.title('Moroccan Real-estate Monitor')
window = st.sidebar.selectbox("Choose a window", ["Dashboard", "Advanced analysis", "AI"])


                  
##### ^^^^^^^^^^^ @@@@@@@@@@@@ Dashboard     @@@@@@@@@@@@ ^^^^^^^^^^^ #####

if window == "Dashboard":
    dashboard(df_merged,df)

##### ^^^^^^^^^^^ @@@@@@@@@@@@   Advanced analysis   @@@@@@@@@@@@ ^^^^^^^^^^^ #####

#if window == "Advanced analysis":

    #advanced_analytics(df)

#if window == "AI":

    #ai(df, feature_store)
