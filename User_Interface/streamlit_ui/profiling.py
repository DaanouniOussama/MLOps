import logging
import streamlit as st
import plotly.graph_objects as go
import psycopg2
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def profiling(df_merged):

    with st.sidebar:
        
        # Add "All Cities" to the city list
        city_list = ['All Cities'] + list(df_merged.ville.unique())[::-1]
        
        selected_city = st.selectbox('Sélectionnez une ville', city_list)

        real_estate_levels = df_merged['real_estate_type'].unique()
        selected_real_estate = st.selectbox('Sélectionnez le type de bien immobilier', sorted(real_estate_levels))

        df_selected_real_estate = df_merged.loc[df_merged['real_estate_type'] == selected_real_estate,:]
        
        # Modify filtering logic to handle "All Cities" option
        if selected_city == 'All Cities':
            df_selected_city = df_merged[df_merged.real_estate_type==selected_real_estate]
        else:
            df_selected_city = df_merged[(df_merged.ville == selected_city) & (df_merged.real_estate_type==selected_real_estate)]

    # Aggregate key metrics by neighborhood (Secteur)
    df_profile = df_selected_city.groupby('secteur').agg(
        mean_price=('price', 'mean'),
        avg_surface_totale=('surface_totale', 'mean'),
        avg_chambres=('chambres', 'mean'),
        perc_terrasse=('terrasse', lambda x: (x == 'Yes').mean() ),
        perc_parking=('parking', lambda x: (x == 'Yes').mean() ),
        perc_ascenseur=('ascenseur', lambda x: (x == 'Yes').mean())
    ).reset_index()

    # Normalize continuous features (price, surface, rooms)
    scaler = MinMaxScaler()
    df_profile[['mean_price', 'avg_surface_totale', 'avg_chambres']] = scaler.fit_transform(
        df_profile[['mean_price', 'avg_surface_totale', 'avg_chambres']]
    )

    # Select a neighborhood (Secteur) to profile
    selected_neighborhood = st.selectbox("Select a neighborhood", df_profile['secteur'].unique())

    # Filter data for the selected neighborhood
    profile_data = df_profile[df_profile['secteur'] == selected_neighborhood]

    # Categories to include in the spider chart
    categories = ['mean_price', 'avg_surface_totale', 'avg_chambres', 
                  'perc_terrasse', 'perc_parking', 'perc_ascenseur']

    # Values for the radar chart
    values = profile_data[categories].values[0]

    # Ensure the radar chart closes the loop by appending the first value to the end
    values = list(values)  # Convert numpy array to list if necessary
    values.append(values[0])  # Append the first value to close the loop

    categories.append(categories[0])  # Append the first category to close the loop

    # Create a spider chart (radar chart) for the selected neighborhood
    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        fillcolor="blue", opacity=0.6, line=dict(color="red"),
        name=selected_neighborhood
    ))

    # Update layout for better readability and fixed axis
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1]),  # Set a fixed range for the radar chart
            angularaxis=dict(rotation=90)  # Rotate the plot to a fixed starting angle
        ),
        title=f"Profiling of {selected_neighborhood}",
        showlegend=False
    )

    # Display the spider chart
    st.plotly_chart(fig)

    # Display raw values for better interpretation
    st.subheader(f"Key Metrics for {selected_neighborhood}")
    st.write(profile_data[categories[:-1]])