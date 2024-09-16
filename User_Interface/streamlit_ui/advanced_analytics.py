import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def advanced_analytics(df_merged,option_site,option_transaction):


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


    #st.write(df_selected_city)
    # Let users select features to group by
    if (option_site == 'avito') and (option_transaction == 'sell'):
        boolean_features = df_merged.columns[14:28]
        selected_boolean_features  = st.multiselect('Select Features to Group By', boolean_features)
    elif (option_site == 'mubawab') and (option_transaction == 'sell'):
        boolean_features = df_merged.columns[13:27]
        selected_boolean_features  = st.multiselect('Select Features to Group By', boolean_features)
    else:
        boolean_features = df_merged.columns[13:27]
        selected_boolean_features  = st.multiselect('Select Features to Group By', boolean_features)

    col1, col2 = st.columns(2)
    with col1:
    # Sliders for Surface Totale with dynamic boundaries
        surface_min = st.slider(
            'Minimum Surface Totale',
            min_value=int(df_selected_city['surface_totale'].min()),
            max_value=int(df_selected_city['surface_totale'].max()),
            value=int(df_selected_city['surface_totale'].min()),
            step=1,
            key='surface_min'
        )

        # Sliders for Chambres with dynamic boundaries
        chambres_min = st.slider(
            'Minimum Chambres',
            min_value=int(df_selected_city['chambres'].min()),
            max_value=int(df_selected_city['chambres'].max()),
            value=int(df_selected_city['chambres'].min()),
            step=1,
            key='chambres_min'
        )
        if option_site == 'avito':
            salons_min = st.slider(
                'Minimum salons',
                min_value=int(df_selected_city['salons'].min()),
                max_value=int(df_selected_city['salons'].max()),
                value=int( df_selected_city['salons'].min()),
                step=1,
                key='salons_min'
            )
        else:
            pieces_min = st.slider(
                'Minimum pieces',
                min_value=int(df_selected_city['pieces'].min()),
                max_value=int(df_selected_city['pieces'].max()),
                value=int( df_selected_city['pieces'].min()),
                step=1,
                key='pieces_min'
            )
        # Sliders for salle de bain with dynamic boundaries
        salle_bains_min = st.slider(
            'Minimum salles de bains',
            min_value=int(df_selected_city['salle_bains'].min()),
            max_value=int(df_selected_city['salle_bains'].max()),
            value=int(df_selected_city['salle_bains'].min()),
            step=1,
            key='salle_bains_min'
        )
        if (option_site == 'mubawab') and (selected_real_estate == 'Villa'):
            pass
        elif option_site == 'avito':
            etage_min = st.slider(
                'Minimum etage',
                min_value=0,
                max_value=int(df_selected_city['etage'].max()),
                value=0,
                step=1,
                key='etage_min'
            )
        else:
            df_selected_city['etage'] = pd.to_numeric(df_selected_city['etage'], errors='coerce')
            etage_min = st.slider(
                'Minimum etage',
                min_value=0,
                max_value=int(df_selected_city['etage'].max(skipna=True)),
                value=0,
                step=1,
                key='etage_min'
            )

        secteur_list = list(df_selected_city.secteur.unique())[::-1]
        selected_secteur = st.selectbox('Sélectionnez le secteur : ', sorted(secteur_list))

        if option_site == 'mubawab':
            etat_list = list(df_selected_city.etat.unique())[::-1]
            selected_etat = st.selectbox("Sélectionnez l'etat du bien : ", etat_list)            
        else:
            pass

    with col2:
        surface_max = st.slider(
            'Maximum Surface Totale',
            min_value=surface_min,  # Limit the min of surface_max to surface_min
            max_value=int(df_selected_city['surface_totale'].max()),
            value=int(df_selected_city['surface_totale'].max()),
            step=1,
            key='surface_max'
        )

        chambres_max = st.slider(
            'Maximum Chambres',
            min_value=chambres_min,  # Limit the min of chambres_max to chambres_min
            max_value=int(df_selected_city['chambres'].max()),
            value=int(df_selected_city['chambres'].max()),
            step=1,
            key='chambres_max'
        )
        if option_site == 'avito':
            salons_max = st.slider(
                'Maximum salons',
                min_value=salons_min,  # Limit the min of chambres_max to chambres_min
                max_value=int(df_selected_city['salons'].max()),
                value=int(df_selected_city['salons'].max()),
                step=1,
                key='salons_max'
            )
        else:
            pieces_max = st.slider(
                'Maximum pieces',
                min_value=pieces_min,  # Limit the min of chambres_max to chambres_min
                max_value=int(df_selected_city['pieces'].max()),
                value=int(df_selected_city['pieces'].max()),
                step=1,
                key='pieces_max'     
            )       

        salle_bains_max = st.slider(
            'Maximum salles de bains',
            min_value=salle_bains_min,  # Limit the min of chambres_max to chambres_min
            max_value=int(df_selected_city['salle_bains'].max()),
            value=int(df_selected_city['salle_bains'].max()),
            step=1,
            key='salle_bains_max'
        )
        if (option_site == 'mubawab') and (selected_real_estate == 'Villa'):
            pass
        elif option_site == 'avito':
            etage_max = st.slider(
                'Maximum etage',
                min_value=etage_min,  # Limit the min of chambres_max to chambres_min
                max_value=int(df_selected_city['etage'].max()),
                value=int(df_selected_city['etage'].max()),
                step=1,
                key='etage_max'
            )
        else: 
            # Replace 'Not_defined' with NaN or a placeholder (e.g., 0)

            etage_max = st.slider(
                'Maximum etage',
                min_value=etage_min,  # Limit the min of chambres_max to chambres_min
                max_value=int(df_selected_city['etage'].max(skipna=True)),
                value=int(df_selected_city['etage'].max()),
                step=1,
                key='etage_max'
            )
        if (option_site == 'avito') and (option_transaction == 'rent'):
            pass
        else:
            age_bien_list = list(df_selected_city.age_bien.unique())[::-1]
            selected_age_bien = st.selectbox("Sélectionnez l'age_bien: ", (age_bien_list))

        
    # Filter the dataframe based on slider values
    if (option_site == 'avito') and (option_transaction == 'rent'):
        filtered_df = df_selected_city[
            (df_selected_city['surface_totale'] >= surface_min) & 
            (df_selected_city['surface_totale'] <= surface_max) &
            (df_selected_city['chambres'] >= chambres_min) & 
            (df_selected_city['chambres'] <= chambres_max) &
            (df_selected_city['salle_bains'] >= salle_bains_min) & 
            (df_selected_city['salle_bains'] <= salle_bains_max) & 
            (df_selected_city['etage'] >= etage_min) & 
            (df_selected_city['etage'] <= etage_max) & 
            (df_selected_city['salons'] >= salons_min) & 
            (df_selected_city['salons'] <= salons_max) &
            (df_selected_city['secteur'] == selected_secteur) 
        ]
    
    elif option_site == 'avito':
        filtered_df = df_selected_city[
            (df_selected_city['surface_totale'] >= surface_min) & 
            (df_selected_city['surface_totale'] <= surface_max) &
            (df_selected_city['chambres'] >= chambres_min) & 
            (df_selected_city['chambres'] <= chambres_max) &
            (df_selected_city['salle_bains'] >= salle_bains_min) & 
            (df_selected_city['salle_bains'] <= salle_bains_max) & 
            (df_selected_city['etage'] >= etage_min) & 
            (df_selected_city['etage'] <= etage_max) & 
            (df_selected_city['salons'] >= salons_min) & 
            (df_selected_city['salons'] <= salons_max) &
            (df_selected_city['secteur'] == selected_secteur) &
            (df_selected_city['age_bien'] == selected_age_bien)
        ]
    elif (option_site == 'mubawab') and (selected_real_estate == 'Villa'):
        filtered_df = df_selected_city[
            (df_selected_city['surface_totale'] >= surface_min) & 
            (df_selected_city['surface_totale'] <= surface_max) &
            (df_selected_city['chambres'] >= chambres_min) & 
            (df_selected_city['chambres'] <= chambres_max) &
            (df_selected_city['salle_bains'] >= salle_bains_min) & 
            (df_selected_city['salle_bains'] <= salle_bains_max) & 
            (df_selected_city['pieces'] >= pieces_min) & 
            (df_selected_city['pieces'] <= pieces_max) &
            (df_selected_city['secteur'] == selected_secteur) &
            (df_selected_city['age_bien'] == selected_age_bien)
        ]        
    else:
        filtered_df = df_selected_city[
            (df_selected_city['surface_totale'] >= surface_min) & 
            (df_selected_city['surface_totale'] <= surface_max) &
            (df_selected_city['chambres'] >= chambres_min) & 
            (df_selected_city['chambres'] <= chambres_max) &
            (df_selected_city['salle_bains'] >= salle_bains_min) & 
            (df_selected_city['salle_bains'] <= salle_bains_max) & 
            (df_selected_city['etage'] >= etage_min) & 
            (df_selected_city['etage'] <= etage_max) & 
            (df_selected_city['pieces'] >= pieces_min) & 
            (df_selected_city['pieces'] <= pieces_max) &
            (df_selected_city['secteur'] == selected_secteur) &
            (df_selected_city['age_bien'] == selected_age_bien) &
            (df_selected_city['etat'] == selected_etat)
        ]

    # Apply 'Yes'/'No' filtering for selected boolean features
    for feature in boolean_features:
        if feature in selected_boolean_features:
            filtered_df = filtered_df[filtered_df[feature] == 'Yes']
        else:
            filtered_df = filtered_df[filtered_df[feature] == 'No']

   # Calculate the mean price based on the filters
    if not filtered_df.empty:
        mean_price = filtered_df['price'].mean()
        num_properties = len(filtered_df)
        
        # Display results in a more aesthetic way
        st.markdown(
            f"""
            <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px;">
                <h3 style="color: #4CAF50;">Filtered Results</h3>
                <p style="font-size: 18px;">Nombre de propriétés correspondant aux critères: <strong>{num_properties}</strong></p>
                <p style="font-size: 18px;">Prix ​​moyen des propriétés filtrées: <strong style="color: #FF5733;">{mean_price:,.2f} MAD</strong></p>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <div style="background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px;">
                <strong>No properties match the selected criteria.</strong>
            </div>
            """,
            unsafe_allow_html=True
        )



    
    
    # Create individual violin plots for each city
    fig = go.Figure()

    for city in list(df_selected_real_estate.ville.unique()):
        fig.add_trace(go.Violin(x=df_selected_real_estate['ville'][df_selected_real_estate['ville'] == city],
                                y=df_selected_real_estate['price'][df_selected_real_estate['ville'] == city],
                                name=city,
                                box_visible=True,
                                meanline_visible=True))

    # Create a combined violin plot for all cities
    fig.add_trace(go.Violin(x=['All Cities'] * len(df_selected_real_estate['price']),
                            y=df_selected_real_estate['price'],
                            name='All Cities',
                            box_visible=True,
                            meanline_visible=True))

    # Update layout
    fig.update_layout(title="Répartition des prix de l'immobilier dans les villes marocaines",
                    yaxis_title='Prix',
                    xaxis_title='Ville',
                    violingap=0.4,
                    violinmode='overlay')

    # Display the plot in Streamlit
    st.plotly_chart(fig)




    # Create individual violin plots for each neighboorhoud
    fig = go.Figure()

    for nb in list(df_selected_city.secteur.unique()):
        fig.add_trace(go.Violin(x=df_selected_city['secteur'][df_selected_city['secteur'] == nb],
                                y=df_selected_city['price'][df_selected_city['secteur'] == nb],
                                name=nb,
                                box_visible=True,
                                meanline_visible=True))

    # Create a combined violin plot for all cities
    fig.add_trace(go.Violin(x=['All Cities'] * len(df_selected_city['price']),
                            y=df_selected_city['price'],
                            name='All Cities',
                            box_visible=True,
                            meanline_visible=True))

    # Update layout
    fig.update_layout(title=f"Répartition des prix de l'immobilier dans les quartiers de {selected_city}",
                    yaxis_title='Prix',
                    xaxis_title='Quartiers',
                    violingap=0.4,
                    violinmode='overlay')

    # Display the plot in Streamlit
    st.plotly_chart(fig)

    # st.markdown('#### Market Insights: Real Estate')
    # # Dropdowns for user to select x and y axes
    # x_axis = st.selectbox('Select the x-axis', options=df_selected_city[['price','superficie','floor','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('superficie'))
    # y_axis = st.selectbox('Select the y-axis', options=df_selected_city[['price','superficie','floor','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('price'))

    # # Create scatter plot based on user selection
    # fig = px.scatter(df_selected_city, x=x_axis, y=y_axis, trendline="ols", hover_data=['rooms', 'floor', 'age'])

    # # Display the plot
    # st.plotly_chart(fig)
