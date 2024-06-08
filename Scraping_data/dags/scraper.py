import logging
from bs4 import BeautifulSoup as soup
import pandas as pd
import httpx
import time 

# floor = [0,1,2,3]
# room = [1,2,3]
# bathroom = [1,2]
def scrapper(city : str)->pd.DataFrame:
    min_price = 250000
    min_size = 30
    real_estate = ['appartements']
    floor = [0,1,2,3]
    room = [1,2,3]
    bathroom = [1,2] # 1 , 2

    # My DataFrame
    my_df = pd.DataFrame()

    for immobilier in real_estate:
        for etage in floor:
            for chambre in room:
                for sl_bain in bathroom:
                    for age in [0,1,2,3,4]:                
                        try:
                            if immobilier != 'appartements':
                                # URL
                                my_url = f"https://www.avito.ma/fr/{city}/{immobilier}-%C3%A0_vendre?price={min_price}-&size={min_size}-&floors={etage}-{etage}&rooms={chambre}-{chambre}&bathrooms={sl_bain}-{sl_bain}&property_age={age}"
                            else :
                                # URL           
                                my_url = f"https://www.avito.ma/fr/{city}/{immobilier}-%C3%A0_vendre?price={min_price}-&size={min_size}-&floor={etage}-{etage}&rooms={chambre}-{chambre}&bathrooms={sl_bain}-{sl_bain}&property_age={age}"
                                
                            # visit the website with the url : my_url
                            response = httpx.get(my_url, timeout=10.0)
                            time.sleep(2)
                            logging.info('Defining links and accessing websites have been successful')

                        except Exception as e:
                            logging.error(f'Error while Defining links and accessing websites : {e}')
                            raise e

                        # Parse html
                        try:
                            html = response.content
                            market_soup = soup(html,'html.parser')
                            logging.info('Parsing html was successful')
                        except Exception as e :
                            logging.error(f'Error while parsing html : {e}')
                            raise e

                        try : 
                            for item_div in market_soup.find_all('div', class_= "sc-b57yxx-1 kBlnTB"):
                                title_element = item_div.find('p', class_='sc-1x0vz2r-0 czqClV')  # Adjust class names
                                adresse = item_div.find('p', class_='sc-1x0vz2r-0 iFQpLP')
                                price_element = item_div.find('p', class_='sc-1x0vz2r-0 eCXWei sc-b57yxx-3 IneBF')  # Adjust class names
                                superficie = item_div.find('div',title='Surface totale')


                                if title_element and adresse and price_element and superficie:  # Check if all elements exist
                                    title_element_ = title_element.text.strip()
                                    adresse_ = adresse.text.strip()
                                    price_element_ = price_element.text.strip()
                                    price_element_ = int(price_element_.replace('DH','').replace(',',''))
                                    superficie_ = superficie.text.strip()
                                    superficie_ = int(superficie_.replace('m²',''))
                                    temp_df = pd.DataFrame({
                                        'title' : [title_element_],
                                        'real_estate_type' : [immobilier],
                                        'price (DHS)' : [price_element_],
                                        'superficie (m²)' : [superficie_],
                                        'Rooms' : [chambre],
                                        'bath_room' : [sl_bain],
                                        'floor' : [etage],
                                        'age' : [age],
                                        'neighbourhood' : [adresse_],
                                        'city' : [city]
                                        })

                                    my_df = pd.concat([my_df, temp_df], ignore_index=True)
                                    time.sleep(1)
                            logging.info('Extracting data and putting it in DataFrame format was successful')
                        except Exception as e:
                            logging.error(f'Error while Extracting Data and putting it in DataFrame format : {e}')
                            raise e
    try:
        my_df.loc[my_df['age']==0,'age'] = 'New'
        my_df.loc[my_df['age']==1,'age'] = '1-5 years'
        my_df.loc[my_df['age']==2,'age'] = '6-10 years'
        my_df.loc[my_df['age']==3,'age'] = '11-21 years'
        my_df.loc[my_df['age']==4,'age'] = '21+ years'
        my_df['neighbourhood'] = my_df['neighbourhood'].apply(lambda x: x.split(',')[-1].strip())
        my_df.to_csv('/opt/airflow/dags/processed_scraped.csv',index=False)
        logging.info('Saving data in csv format was successful')

    except Exception as e:
        logging.error(f'Error while saving data into csv format : {e}')
    return 'Scraping and processing & staging data steps were completed'
