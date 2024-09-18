import streamlit as st
import pandas as pd
import plotly.express as px

def trends(df_merged, option_site, option_transaction):
    # Sidebar for filtering options
    with st.sidebar:
        # Add "All Cities" to the city list
        city_list = ['All Cities'] + list(df_merged['ville'].unique())[::-1]
        selected_city = st.selectbox('Sélectionnez une ville', city_list)

        real_estate_levels = df_merged['real_estate_type'].unique()
        selected_real_estate = st.selectbox('Sélectionnez le type de bien immobilier', sorted(real_estate_levels))

        df_selected_real_estate = df_merged.loc[df_merged['real_estate_type'] == selected_real_estate, :]

        # Modify filtering logic to handle "All Cities" option
        if selected_city == 'All Cities':
            df_selected_city = df_merged[df_merged['real_estate_type'] == selected_real_estate]
        else:
            df_selected_city = df_merged[(df_merged['ville'] == selected_city) & (df_merged['real_estate_type'] == selected_real_estate)]


    # Convert 'unix_time' to datetime if not already in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_selected_city['unix_time']):
        df_selected_city['Date'] = pd.to_datetime(df_selected_city['unix_time'], unit='s')
    else:
        df_selected_city['Date'] = df_selected_city['unix_time']

    df_selected_city.set_index('Date', inplace=True)

    # Sidebar: Additional filter options
    st.sidebar.header("Filter Options")

    # Secteur
    secteur = st.sidebar.selectbox("Secteur", ['All'] + df_selected_city['secteur'].unique().tolist())

    # Surface Totale (min and max slider)
    min_surface_totale, max_surface_totale = st.sidebar.slider(
        "Surface Totale (min, max)",
        int(df_selected_city['surface_totale'].min()),
        int(df_selected_city['surface_totale'].max()),
        (int(df_selected_city['surface_totale'].min()), int(df_selected_city['surface_totale'].max()))
    )

    # Chambres
    chambres = st.sidebar.selectbox("Chambres", ['All'] + df_selected_city['chambres'].unique().tolist())

    # Terrasse
    terrasse = st.sidebar.selectbox("Terrasse", ['All', 'Yes', 'No'])

    # Filter the data based on user selection
    filtered_df = df_selected_city.copy()

    # Apply the filters
    if secteur != 'All':
        filtered_df = filtered_df[filtered_df['secteur'] == secteur]

    filtered_df = filtered_df[(filtered_df['surface_totale'] >= min_surface_totale) & 
                              (filtered_df['surface_totale'] <= max_surface_totale)]

    if chambres != 'All':
        filtered_df = filtered_df[filtered_df['chambres'] == int(chambres)]

    if terrasse != 'All':
        filtered_df = filtered_df[filtered_df['terrasse'] == ('Yes' if terrasse == 'Yes' else 'No')]

    # Main section: Display the filtered data and plot the price trend
    st.title("Real Estate Price Trend Analysis")

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
    else:
        st.write(f"Showing data for {len(filtered_df)} properties.")
        
        # Select only numeric columns for resampling and calculating the mean
        numeric_columns = filtered_df.select_dtypes(include=['number']).columns
        filtered_df_numeric = filtered_df[numeric_columns]

        # Resample the numeric data by day and forward-fill missing values to avoid gaps
        filtered_df_resampled = filtered_df_numeric.resample('D').mean().ffill()

        # Plotting price trend using Plotly
        fig = px.line(filtered_df_resampled, x=filtered_df_resampled.index, y='price', title="Historical Price Trend")
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Average Price",
            template="plotly_white"
        )
        
        st.plotly_chart(fig)