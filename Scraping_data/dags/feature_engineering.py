import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to determine the quantile code
def assign_quantile_code(price, quantiles):
    for i, q in enumerate(quantiles):
        if price <= q:
            return i
    return len(quantiles) + 1

def preprocess() -> pd.DataFrame:
    try:
        data = pd.read_csv('/opt/airflow/dags/scraped_data.csv',index_col=0)
        logging.info('Feature engineering column neighbourhood')
        data['neighbourhood_city'] = data['neighbourhood'] + ', ' + data['city']
        data = data[['real_estate_type','superficie (m²)','Rooms','bath_room','floor','age','neighbourhood_city','city','price (DHS)']]
        logging.info('Deleting outliers')
        lower_quantile = data.groupby('neighbourhood_city')['price (DHS)'].quantile(0.05).reset_index()
        upper_quantile = data.groupby('neighbourhood_city')['price (DHS)'].quantile(0.95).reset_index()
        lower_quantile.rename(columns={'price (DHS)': '0.05_price'}, inplace=True)
        upper_quantile.rename(columns={'price (DHS)': '0.95_price'}, inplace=True)
        adding_2 = pd.merge(data , lower_quantile, on='neighbourhood_city', how='left')
        final_df = pd.merge(adding_2 , upper_quantile, on='neighbourhood_city', how='left')
        final_df = final_df[(final_df['price (DHS)']>final_df['0.05_price']) & (final_df['price (DHS)']<final_df['0.95_price'])]
        final_df = final_df.iloc[:,:-2]
    
    except Exception as e:
        logging.error(f'Error while cleaning data : {e}')
        raise e

    try:
        logging.info('Coding cities')
        final_df.loc[final_df['city']=='casablanca','city'] = 4
        final_df.loc[final_df['city']=='tanger','city'] = 3
        final_df.loc[final_df['city']=='marrakech','city'] = 2
        final_df.loc[final_df['city']=='agadir','city'] = 1
        final_df.loc[final_df['city']=='rabat','city'] = 0
        logging.info('Coding ages')
        final_df.loc[final_df['age']=='New','age'] = 0
        final_df.loc[final_df['age']=='1-5 years','age'] = 1
        final_df.loc[final_df['age']=='6-10 years','age'] = 2
        final_df.loc[final_df['age']=='11-21 years','age'] = 3
        final_df.loc[final_df['age']=='21+ years','age'] = 4
        logging.info('Coding type of real-estate')
        final_df.loc[final_df['real_estate_type']=='appartements','real_estate_type'] = 0
        final_df.loc[final_df['real_estate_type']=='maisons','real_estate_type'] = 1
        final_df.loc[final_df['real_estate_type']=='villas_riad','real_estate_type'] = 2
        logging.info('Label coding the neighboorhoods')

        
        # calculate quartile 10 
        quantiles = final_df.loc[:,'price (DHS)'].quantile([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])

        median_price_df = final_df.groupby('neighbourhood_city')['price (DHS)'].median()

        # Apply the function to assign quantile code to each neighborhood
        median_price_df['quantile_code'] = median_price_df.apply(assign_quantile_code, quantiles=quantiles)

        final_df = final_df.merge(median_price_df['quantile_code'],left_on='neighbourhood_city', right_on='neighbourhood_city')

        final_df.to_csv('/opt/airflow/dags/staging_data2.csv',index=False)

        final_df = final_df[['real_estate_type','superficie (m²)','Rooms','bath_room','floor','age','price (DHS)_y','neighbourhood_city','city','price (DHS)_x']]

        final_df = final_df.rename(columns={'superficie (m²)' : 'superficie', 'Rooms' : 'rooms',"price (DHS)_y":"neighbourhood_city_coded","price (DHS)_x":"price"})

        final_df.to_csv('/opt/airflow/dags/processed_scraped.csv',index=False)
    except Exception as e:
        logging.error(f'Error while coding variables : {e}')
        raise e
    