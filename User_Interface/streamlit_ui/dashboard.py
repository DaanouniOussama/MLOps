import streamlit as st
import plotly.express as px
import pandas as pd
import pydeck as pdk
import altair as alt

def dashboard(df_merged,df):

    with st.sidebar:
        
        # Add "All Cities" to the city list
        city_list = ['All Cities'] + list(df.ville.unique())[::-1]
        
        selected_city = st.selectbox('Sélectionnez une ville', city_list)

        real_estate_levels = df['real_estate_type'].unique()
        selected_real_estate = st.selectbox('Sélectionnez le type de bien immobilier', sorted(real_estate_levels))
        
        # Modify filtering logic to handle "All Cities" option
        if selected_city == 'All Cities':
            df_selected_city = df[df.real_estate_type==selected_real_estate]
        else:
            df_selected_city = df[(df.ville == selected_city) & (df.real_estate_type==selected_real_estate)]



    # Define the layer for the map
    layer = pdk.Layer(
        'HeatmapLayer',
        data=df_merged,
        get_position='[longitude, latitude]',
        get_radius=10,  # Fixed radius, can be adjusted as needed
        get_fill_color='color',
        pickable=True,
        opacity=0.5
    )

    # Set the view state
    view_state = pdk.ViewState(
        latitude=31.7917,
        longitude=-7.0926,
        zoom=5,
        pitch=0
    )

    # Create the deck.gl map
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "Price: {price}"}
        
    )
    # Dashboard Main Panel
    col = st.columns((1.6, 4.5, 2), gap='medium')
    with col[0]:
        st.markdown('###### Prix moyen du mètre carré par ville')
        #st.write(df_selected_city)
        #st.write(df_selected_city)
        st.metric(str(df_selected_city.ville.unique()[0]),f"{int(df_selected_city.price.mean()/df_selected_city.surface_totale.mean()):,}".replace(',', ' ') + " MAD",delta='',delta_color='normal')
        neighbourhood = st.selectbox('Sélectionnez le quartier',sorted(df_selected_city.secteur.unique()))
        st.metric('Prix ​​moyen du mètre carré',f"{int(df_selected_city[df_selected_city['secteur']==neighbourhood].price.mean()/df_selected_city[df_selected_city['secteur']==neighbourhood].surface_totale.mean()):,}".replace(',', ' ') + " MAD",delta='',delta_color='normal')


    with col[1]:
        st.markdown("##### Répartition géographique des prix de l'immobilier au Maroc")
        #st.write(df)
        # Render the map in Streamlit
        st.pydeck_chart(r)

        #st.write(df_merged)



    with col[2]:
        st.markdown("##### La distribution des prix de l'immobilier")

        # Create a histogram with KDE line using Plotly
        fig = px.histogram(df_selected_city, x='price', nbins=50, marginal='box', title=f'La Distribution des prix a {selected_city}')

        # Display the histogram with KDE line
        st.plotly_chart(fig)

        # category = df.real_estate_type.unique()
        # #st.write(category)
        # if selected_city == 'All Cities':
        #     value = df['real_estate_type'].value_counts(normalize=True)
        # else:
        #     value = df.loc[df['ville']==selected_city,'real_estate_type'].value_counts(normalize=True)
        # #st.write(value)
        # value = (value*100).round(1)  # Round to one decimal place
        # source = pd.DataFrame({"category": category, "Pourcentage": value.values})

        # chart = alt.Chart(source).mark_arc(innerRadius=50).encode(
        #     theta=alt.Theta(field="Pourcentage", type="quantitative"),
        #     color=alt.Color(field="category", type="nominal"),
        # )

        # st.altair_chart(chart, theme=None, use_container_width=True)
                