import mlflow
import os
import pandas as pd
import streamlit as st
import logging
from sklearn.preprocessing import LabelEncoder



def ai(df_merged):

    os.environ['MLFLOW_TRACKING_USERNAME'] = 'DaanouniOussama'
    os.environ['MLFLOW_TRACKING_PASSWORD'] = '70f0d514b2b8dc0829ed8030f38eb9421734bbcc'

    remote_server_uri = "https://dagshub.com/DaanouniOussama/MLOps.mlflow"

    mlflow.set_tracking_uri(remote_server_uri)

    logged_model = 'runs:/97f2e0a964ea475e8d71977fec719138/RF_model'

    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)

    st.markdown('#### Predict the price of your real-estate')


    real_estate_type = st.selectbox('Please select type of your real-estate', ['Appartement'])
    area = st.number_input('Please enter the area of your real estate', min_value=20, format="%d")
    city = st.selectbox('Please select the city of your real-estate', ['Casablanca', 'Rabat', 'Tanger', 'Marrakech', 'Agadir'])
        
    logging.info('Coding cities')
    # Create a mapping dictionary (mask)
    if(city=='Casablanca'):
        city_ = 5
    elif( city =='Tanger'):
        city_ = 4
    elif( city == 'Marrakech' ):
        city_ = 3
    elif( city == 'Agadir' ):
        city_ = 2
    else:
        city_ = 1
    # here encode neighbourhood
    city = city.lower()
    label_encoder = LabelEncoder()
    # Fit and transform the neighborhood column
    label_encoder.fit(df_merged['neighbourhood_city'])
    neighbourhood = st.selectbox('Please select the neighbourhood of your real-estate', df_merged.loc[df_merged['city']==city,'neighbourhood'].unique())
    neighbourhood_encoded = label_encoder.transform([neighbourhood + ', ' + city])
    age = st.number_input('Please enter the age of your real estate', min_value=0, format="%d")
    floor = st.number_input('Please enter the floor of your real estate', min_value=0, format="%d")
    rooms = st.number_input('Please enter the number of rooms of your real estate', min_value=0, format="%d")
    bath_rooms = st.number_input('Please enter the number of bathrooms of your real estate', min_value=0, format="%d")



    logging.info('Coding ages')

    if(age==0):
        age_ = 0
    elif( (age>=1) & (age<=5) ):
        age_ = 1
    elif( (age>=6) & (age<=10) ):
        age_ = 2
    elif( (age>=11) & (age<=21) ):
        age_ = 3
    else:
        age_ = 4

    logging.info('Label coding the neighboorhoods')





    if st.button("Predict"):
        # data
        data = pd.DataFrame({'superficie' : [area] , 
                         'rooms' : [rooms],
                         'bath_room' : [bath_rooms],
                         'floor' : [floor],
                         'age' : [age_],
                         'neighbourhood_' : neighbourhood_encoded,
                         'city' : [city_]                       
                         })
        # Predict on a Pandas DataFrame.
        predicted_price = loaded_model.predict(data)
        st.write('The price of your real-estate : ', int(predicted_price[0]), 'Dhs')





