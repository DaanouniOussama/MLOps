import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import plotly.express as px

def profiling(df_merged, option_site, option_transaction):
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

    # Selecting extras features for each DB
    if (option_site == 'avito') and (option_transaction == 'sell'):
        boolean_features = df_merged.columns[14:28]
    else:
        boolean_features = df_merged.columns[13:27]

    # Ensure columns are present in the dataframe (in case they don't exist)
    boolean_features = [col for col in boolean_features if col in df_merged.columns]

    # Prepare the aggregation logic for boolean features
    boolean_agg = {feature: (feature, lambda x: (x == 'Yes').mean()) for feature in boolean_features}

    # Aggregate key metrics by neighborhood (Secteur)
    df_profile = df_selected_city.groupby('secteur').agg(
        mean_price=('price', 'mean'),
        avg_surface_totale=('surface_totale', 'mean'),
        avg_chambres=('chambres', 'mean'),
        **boolean_agg  # Add the dynamically created boolean feature aggregations
    ).reset_index()

    # Normalize continuous features (price, surface, rooms)
    scaler = MinMaxScaler()
    df_profile[['mean_price', 'avg_surface_totale', 'avg_chambres']] = scaler.fit_transform(
        df_profile[['mean_price', 'avg_surface_totale', 'avg_chambres']]
    )

    # Select multiple neighborhoods (Secteurs) to profile
    selected_neighborhoods = st.multiselect("Select neighborhoods to compare", df_profile['secteur'].unique())

    # Categories to include in the spider chart (continuous + dynamic boolean features)
    categories = ['mean_price', 'avg_surface_totale', 'avg_chambres'] + boolean_features
    categories.append(categories[0])  # Append the first category to close the loop

    # Get a color palette for the number of selected neighborhoods
    color_palette = px.colors.qualitative.Plotly  # You can use other palettes like 'D3', 'Set1', etc.
    color_cycle = iter(color_palette)

    # Create a spider chart (radar chart) for the selected neighborhoods
    fig = go.Figure()

    for neighborhood in selected_neighborhoods:
        # Filter data for the selected neighborhood
        profile_data = df_profile[df_profile['secteur'] == neighborhood]

        # Values for the radar chart
        values = profile_data[categories[:-1]].values[0]  # Exclude the repeated first category
        values = list(values)  # Convert numpy array to list if necessary
        values.append(values[0])  # Append the first value to close the loop

        # Assign a unique color to each neighborhood
        color = next(color_cycle)

        # Add the neighborhood's trace to the spider chart
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            fillcolor=color,  # Use the generated color
            opacity=0.4,
            line=dict(color=color),
            name=neighborhood
        ))

    # Update layout for better readability and fixed axis, increase figure size
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1]),  # Set a fixed range for the radar chart
            angularaxis=dict(rotation=90)  # Rotate the plot to a fixed starting angle
        ),
        title=f"Comparison of Selected Neighborhoods",
        showlegend=True,
        width=900,  # Increase width
        height=700   # Increase height
    )

    # Display the spider chart
    st.plotly_chart(fig)

    # Display raw values for each selected neighborhood
    for neighborhood in selected_neighborhoods:
        st.subheader(f"Key Metrics for {neighborhood}")
        profile_data = df_profile[df_profile['secteur'] == neighborhood]
        st.write(profile_data[categories[:-1]])  # Exclude the repeated first category
