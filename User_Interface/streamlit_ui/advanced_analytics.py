import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


def advanced_analytics(df_merged):

    with st.sidebar:
    
        # Add "All Cities" to the city list
        city_list = ['All Cities'] + list(df_merged.city.unique())[::-1]
        
        selected_city = st.selectbox('Select a city', city_list)
        
        # Modify filtering logic to handle "All Cities" option
        if selected_city == 'All Cities':
            df_selected_city = df_merged
        else:
            df_selected_city = df_merged[df_merged.city == selected_city]

    # Create individual violin plots for each city
    fig = go.Figure()

    for city in list(df_merged.city.unique()):
        fig.add_trace(go.Violin(x=df_merged['city'][df_merged['city'] == city],
                                y=df_merged['price'][df_merged['city'] == city],
                                name=city,
                                box_visible=True,
                                meanline_visible=True))

    # Create a combined violin plot for all cities
    fig.add_trace(go.Violin(x=['All Cities'] * len(df_merged['price']),
                            y=df_merged['price'],
                            name='All Cities',
                            box_visible=True,
                            meanline_visible=True))

    # Update layout
    fig.update_layout(title='Real Estate Price Distribution Across Moroccan Cities',
                    yaxis_title='Price',
                    xaxis_title='City',
                    violingap=0.4,
                    violinmode='overlay')

    # Display the plot in Streamlit
    st.plotly_chart(fig)


    st.markdown('#### Market Insights: Real Estate')
    # Dropdowns for user to select x and y axes
    x_axis = st.selectbox('Select the x-axis', options=df_selected_city[['price','superficie','floor','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('superficie'))
    y_axis = st.selectbox('Select the y-axis', options=df_selected_city[['price','superficie','floor','rooms','bath_room']].columns, index=df_selected_city.columns.get_loc('price'))

    # Create scatter plot based on user selection
    fig = px.scatter(df_selected_city, x=x_axis, y=y_axis, trendline="ols", hover_data=['rooms', 'floor', 'age'])

    # Display the plot
    st.plotly_chart(fig)
