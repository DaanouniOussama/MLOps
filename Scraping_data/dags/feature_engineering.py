import logging
import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess() -> pd.DataFrame:
    try:
        data = pd.read_csv('/opt/airflow/dags/scraped_data.csv',index_col=0)
        logging.info('Feature engineering column neighbourhood')
        data['neighbourhood_city'] = data['neighbourhood'] + ', ' + data['city']
        data = data[['superficie (m²)','Rooms','bath_room','floor','age','neighbourhood_city','city','price (DHS)']]
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
        final_df.loc[final_df['city']=='casablanca','city'] = 5
        final_df.loc[final_df['city']=='tanger','city'] = 4
        final_df.loc[final_df['city']=='marrakech','city'] = 3
        final_df.loc[final_df['city']=='agadir','city'] = 2
        final_df.loc[final_df['city']=='rabat','city'] = 1
        logging.info('Coding ages')
        final_df.loc[final_df['age']=='New','age'] = 0
        final_df.loc[final_df['age']=='1-5 years','age'] = 1
        final_df.loc[final_df['age']=='6-10 years','age'] = 2
        final_df.loc[final_df['age']=='11-21 years','age'] = 3
        final_df.loc[final_df['age']=='21+ years','age'] = 4
        logging.info('Label coding the neighboorhoods')
        label_encoder = LabelEncoder()
        # Fit and transform the neighborhood column
        final_df['neighbourhood_city'] = label_encoder.fit_transform(final_df['neighbourhood_city'])

        final_df.to_csv('/opt/airflow/dags/processed_scraped.csv',index=False)
    except Exception as e:
        logging.error(f'Error while coding variables : {e}')
        raise e
    