import streamlit as st
import plotly.express as px
import pandas as pd
import pydeck as pdk
import altair as alt

def dashboard(df_merged,df):

    with st.sidebar:
        
        # Add "All Cities" to the city list
        city_list = ['All Cities'] + list(df.city.unique())[::-1]
        
        selected_city = st.selectbox('Select a city', city_list)

        real_estate_levels = df['real_estate_type'].unique()
        selected_real_estate = st.selectbox('Select the type of real-estate', real_estate_levels)
        
        # Modify filtering logic to handle "All Cities" option
        if selected_city == 'All Cities':
            df_selected_city = df[df.real_estate_type==selected_real_estate]
        else:
            df_selected_city = df[(df.city == selected_city) & (df.real_estate_type==selected_real_estate)]



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
    # Dashboard Main Panel
    col = st.columns((1.5, 4.5, 2), gap='medium')
    with col[0]:
        st.markdown('#### Mean price of square meter by city')
        #st.write(df_merged)
        #st.write(df_selected_city)
        st.metric(str(df_selected_city.city.unique()[0]),int(df_selected_city.price.mean()/df_selected_city.superficie.mean()),delta='10K',delta_color='normal')
        neighbourhood = st.selectbox('Select neighbourhood',df_selected_city.neighbourhood.unique())
        st.metric('Mean price of square meter', int(df_selected_city[df_selected_city['neighbourhood']==neighbourhood].price.mean()/df_selected_city[df_selected_city['neighbourhood']==neighbourhood].superficie.mean()),delta='10K',delta_color='normal')


    with col[1]:
        st.markdown('#### Geographic Distribution of Property Prices in Morocco')
        st.write(df)
        # Render the map in Streamlit
        st.pydeck_chart(r)

        #st.write(df_merged)

        # Create a histogram with KDE line using Plotly
        fig = px.histogram(df_selected_city, x='price', nbins=50, marginal='box', title=f'Price Distribution in {selected_city}')

        # Display the histogram with KDE line
        st.plotly_chart(fig)

    with col[2]:
        st.markdown('#### Real-Estate offer type distribution')

        category = df.real_estate_type.unique()
        #st.write(category)
        value = df.loc[df['city']==selected_city,'real_estate_type'].value_counts()
        #st.write(value)


        source = pd.DataFrame({"category": category, "value": value})

        chart = alt.Chart(source).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="value", type="quantitative"),
            color=alt.Color(field="category", type="nominal"),
        )

        st.altair_chart(chart, theme=None, use_container_width=True)
                