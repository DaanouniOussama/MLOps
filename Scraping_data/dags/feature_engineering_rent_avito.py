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

def preprocess_rent() -> pd.DataFrame:
    try:
        data = pd.read_csv('/opt/airflow/dags/scraped_data_rent_avito.csv',index_col=0)
        logging.info('Feature engineering column secteur')
        data['ville_secteur'] = data['secteur'] + ', ' + data['ville']
        # titre,Type,transaction,ville,secteur,surface_totale,surface_habitable,chambres,salle_bains,salons,pieces,age_bien,
        # terrasse,balcon,parking,ascenseur,securite,climatisation,cuisine_equipee,concierge,duplex,chauffage,meuble,prix
        final_df = data[['Type','transaction','ville','ville_secteur','surface_totale','surface_habitable','chambres','salle_bains'
                     ,'salons', 'pieces', 'terrasse', 'balcon', 'parking', 'ascenseur', 'securite', 'climatisation','cuisine_equipee'
                     , 'concierge', 'duplex', 'chauffage', 'meuble', 'garage', 'jardin', 'piscine', 'prix']]
        # logging.info('Deleting outliers')
        # lower_quantile = data.groupby('ville_secteur')['prix'].quantile(0.00005).reset_index()
        # upper_quantile = data.groupby('ville_secteur')['prix'].quantile(0.99995).reset_index()
        # lower_quantile.rename(columns={'prix': '0.005_price'}, inplace=True)
        # upper_quantile.rename(columns={'prix': '0.995_price'}, inplace=True)
        # adding_2 = pd.merge(data , lower_quantile, on='ville_secteur', how='left')
        # final_df = pd.merge(adding_2 , upper_quantile, on='ville_secteur', how='left')
        # final_df = final_df[(final_df['prix']>final_df['0.005_price']) & (final_df['prix']<final_df['0.995_price'])]
        # final_df = final_df.iloc[:,:-2]
    
    except Exception as e:
        logging.error(f'Error while cleaning data : {e}')
        raise e

    try:
        logging.info('Coding cities')
        final_df.loc[final_df['ville']=='Casablanca','ville'] = 4
        final_df.loc[final_df['ville']=='Tanger','ville'] = 3
        final_df.loc[final_df['ville']=='Marrakech','ville'] = 2
        final_df.loc[final_df['ville']=='Agadir','ville'] = 1
        final_df.loc[final_df['ville']=='Rabat','ville'] = 0

        logging.info('Coding type of real-estate')
        final_df.loc[final_df['Type']=='Appartements','Type'] = 0
        final_df.loc[final_df['Type']=='Villas et Riads','Type'] = 1
        logging.info('Coding extras')
        # terrasse
        final_df.loc[final_df['terrasse']=='Yes','terrasse'] = 1
        final_df.loc[final_df['terrasse']=='No','terrasse'] = 0
        # balcon
        final_df.loc[final_df['balcon']=='Yes','balcon'] = 1
        final_df.loc[final_df['balcon']=='No','balcon'] = 0    
        # parking
        final_df.loc[final_df['parking']=='Yes','parking'] = 1
        final_df.loc[final_df['parking']=='No','parking'] = 0 
        # ascenseur        
        final_df.loc[final_df['ascenseur']=='Yes','ascenseur'] = 1
        final_df.loc[final_df['ascenseur']=='No','ascenseur'] = 0    
        # securite    
        final_df.loc[final_df['securite']=='Yes','securite'] = 1
        final_df.loc[final_df['securite']=='No','securite'] = 0 
        # climatisation
        final_df.loc[final_df['climatisation']=='Yes','climatisation'] = 1
        final_df.loc[final_df['climatisation']=='No','climatisation'] = 0 
        # cuisine_equipee
        final_df.loc[final_df['cuisine_equipee']=='Yes','cuisine_equipee'] = 1
        final_df.loc[final_df['cuisine_equipee']=='No','cuisine_equipee'] = 0
        # concierge
        final_df.loc[final_df['concierge']=='Yes','concierge'] = 1
        final_df.loc[final_df['concierge']=='No','concierge'] = 0 
        # duplex      
        final_df.loc[final_df['duplex']=='Yes','duplex'] = 1
        final_df.loc[final_df['duplex']=='No','duplex'] = 0 
        # chauffage
        final_df.loc[final_df['chauffage']=='Yes','chauffage'] = 1
        final_df.loc[final_df['chauffage']=='No','chauffage'] = 0 
        # meuble
        final_df.loc[final_df['meuble']=='Yes','meuble'] = 1
        final_df.loc[final_df['meuble']=='No','meuble'] = 0 
        # garage
        final_df.loc[final_df['garage']=='Yes','garage'] = 1
        final_df.loc[final_df['garage']=='No','garage'] = 0
        # jardin
        final_df.loc[final_df['jardin']=='Yes','jardin'] = 1
        final_df.loc[final_df['jardin']=='No','jardin'] = 0
        # piscine
        final_df.loc[final_df['piscine']=='Yes','piscine'] = 1
        final_df.loc[final_df['piscine']=='No','piscine'] = 0

        # Calculate the median price grouped by 'ville_secteur' and 'type'
        median_price_df = final_df.groupby(['ville_secteur', 'Type'])['prix'].median().reset_index()

        # Initialize a new column for quantile codes
        median_price_df['quantile_code'] = 0

        # Calculate quantiles and assign quantile codes for each 'type'
        for t in final_df['Type'].unique():
            # Calculate quantiles for the current type
            quantiles = final_df[final_df['Type'] == t]['prix'].quantile([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
            
            # Apply the quantile assignment function to the median prices for the current type
            median_price_df.loc[median_price_df['Type'] == t, 'quantile_code'] = median_price_df[median_price_df['Type'] == t]['prix'].apply(assign_quantile_code, quantiles=quantiles)

        # Merge the quantile code back to the original DataFrame
        final_df = final_df.merge(median_price_df[['ville_secteur', 'Type', 'quantile_code']], on=['ville_secteur', 'Type'])

        # Rename columns
        final_df = final_df.rename(columns={"prix_x": "price", "prix_y": "ville_secteur_coded"})

        final_df.to_csv('/opt/airflow/dags/processed_scraped_avito_rent.csv',index=False)

    except Exception as e:
        logging.error(f'Error while coding variables : {e}')
        raise e
    