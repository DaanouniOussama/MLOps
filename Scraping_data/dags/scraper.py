import logging
from bs4 import BeautifulSoup as soup
import pandas as pd
import httpx
import time 


def scrapper(real_estate : list = ['appartements'], city : list = ['casablanca','rabat'], min_price : int = 250000, min_size :int = 30, floor : int = 1)->pd.DataFrame:
    
    # My DataFrame
    my_df = pd.DataFrame()
    

    
    for ville in city:
        for immobilier in real_estate:
            try:
                if immobilier != 'appartements':
                    # URL
                    my_url = f"https://www.avito.ma/fr/{ville}/{immobilier}-%C3%A0_vendre?price={min_price}-&size={min_size}-&floors={floor}"
                else :
                    # URL
                    my_url = f"https://www.avito.ma/fr/{ville}/{immobilier}-%C3%A0_vendre?price={min_price}-&size={min_size}-&floor={floor}"
                # visit the website with the url : my_url
                response = httpx.get(my_url)
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
                    price_element = item_div.find('p', class_='sc-1x0vz2r-0 eCXWei sc-b57yxx-3 IneBF')  # Adjust class names
                    superficie = item_div.find('div',title='Surface totale')


                    if title_element and price_element and superficie:  # Check if all elements exist
                        title_element_ = title_element.text.strip()
                        price_element_ = price_element.text.strip()
                        price_element_ = int(price_element_.replace('DH','').replace(',',''))
                        superficie_ = superficie.text.strip()
                        superficie_ = int(superficie_.replace('m²',''))
                        temp_df = pd.DataFrame({
                            'Title' : [title_element_],
                            'real_estate_type' : [immobilier],
                            'price (DHS)' : [price_element_],
                            'superficie (m²)' : [superficie_],
                            'floor' : [floor],
                            'city' : [ville]
                            })

                        my_df = pd.concat([my_df, temp_df], ignore_index=True)
                        time.sleep(1)
                logging.info('Extracting data and putting it in DataFrame format was successful')
            except Exception as e:
                logging.error(f'Error while Extracting Data and putting it in DataFrame format : {e}')
                raise e
    try:
        my_df.to_csv('/opt/airflow/dags/processed_scraped.csv',index=False)
        logging.info('Saving data in csv format was successful')

    except Exception as e:
        logging.error(f'Error while saving data into csv format : {e}')
    return 'Scraping and processing & staging data steps were completed'


if __name__ == '__main__' :
    # define variable of the function
    #real_estate = ['appartements','Maisons', 'villas_riad']
    #city = ['casablanca','rabat']
    #min_price = 250000
    #min_size = 30
    #floor = 1
    scrapper()

    