import logging
import streamlit as st
import plotly.express as px
import psycopg2
import pandas as pd
import pydeck as pdk
import altair as alt
#import folium
#from geopy.geocoders import Nominatim
#from folium import plugins



# Connecting and Fetching all rows from database 
try:
    logging.info('Connecting to postgres DB ...')
    connection = psycopg2.connect(database="Real_estate", user="airflow", password="airflow", host="localhost", port=54320)
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

# Display data
#st.write(df)
# Plot data
#fig = px.scatter(df, x = 'superficie', y = 'price')
#st.plotly_chart(fig)

# Connecting and Fetching all rows from database Maps_data
try:
    logging.info('Connecting to postgres DB ...')
    connection = psycopg2.connect(database="Real_estate", user="airflow", password="airflow", host="localhost", port=54320)
    query = """ SELECT * FROM maps_table;"""
    df_maps = pd.read_sql_query(query, connection)
    logging.info('Connection to postgres db was successful')

except Exception as e:
    logging.error('error while connecting and extracting data from Postgres DB')
    raise e


df_maps = df_maps.dropna()

df_maps = df_maps.rename(columns={"laltitude": "longitude" , "longitude" :"latitude"})
#df_maps = df_maps[['latitude','longitude']]
df_maps = df_maps.loc[(df_maps['latitude']>21.0) & (df_maps['latitude']<36.0) & (df_maps['longitude']>-17.0) & (df_maps['longitude']<-1.0)]
#st.write(df_maps[['latitude','longitude']])
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

st.set_page_config(
    page_title="Moroccan Real-estate Monitor",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded")


with st.sidebar:
    st.title('Moroccan Real-estate Monitor')
    
    # Add "All Cities" to the city list
    city_list = ['All Cities'] + list(df_merged.city.unique())[::-1]
    
    selected_city = st.selectbox('Select a city', city_list)
    
    # Modify filtering logic to handle "All Cities" option
    if selected_city == 'All Cities':
        df_selected_city = df_merged
    else:
        df_selected_city = df_merged[df_merged.city == selected_city]





# Define the layer for the map
layer = pdk.Layer(
    'HeatmapLayer',
    data=df_merged,
    get_position='[longitude, latitude]',
    get_radius=2000,  # Fixed radius, can be adjusted as needed
    get_fill_color='color',
    pickable=True
)

# Set the view state
view_state = pdk.ViewState(
    latitude=31.7917,
    longitude=-7.0926,
    zoom=3,
    pitch=0
)

# Create the deck.gl map
r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "Price: {price}"}
)

# Render the map in Streamlit
st.pydeck_chart(r)

st.write(df_merged)

#st.subheader("Price vs. Superficie")

# Dropdowns for user to select x and y axes
x_axis = st.selectbox('Select the x-axis', options=df_selected_city[['price','superficie','floor','age','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('superficie'))
y_axis = st.selectbox('Select the y-axis', options=df_selected_city[['price','superficie','floor','age','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('price'))

# Create scatter plot based on user selection
fig = px.scatter(df_selected_city, x=x_axis, y=y_axis, trendline="ols", hover_data=['rooms', 'floor', 'age'])

# Display the plot
st.plotly_chart(fig)