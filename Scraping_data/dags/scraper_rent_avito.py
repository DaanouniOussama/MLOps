import logging
from bs4 import BeautifulSoup as soup
import pandas as pd
import httpx
import time 
import re


def scrapper_rent()->pd.DataFrame:
    min_price = 2500
    max_price = 70000
    min_size = 30
    max_size = 1500
    real_estate = [ 'villas_riad' , 'appartements'] # 'appartements' , villas_riad 
    # extra=balcony,elevator,terrace,heater,airconditioner,furnished,furnished_kitchen,janitor,duplex,parking,security,phone_cable
    cities = ['casablanca']  # 'tanger', 'rabat', 'marrakech', 'agadir'

    # My DataFrame
        # My DataFrame
    my_df = pd.DataFrame(columns=['titre', 'Type', 'transaction', 'ville', 'secteur', 'surface_totale', 
                                  'surface_habitable', 'chambres', 'salle_bains', 'salons',
                                  'pieces', 'etage', 'terrasse', 'balcon', 'parking', 
                                  'ascenseur', 'securite', 'climatisation', 'cuisine_equipee',
                                 'concierge', 'duplex', 'chauffage', 'meuble','garage', 'jardin', 'piscine', 'prix'])

    for city in cities:
        for immobilier in real_estate:               
            try:
                # URL
                my_url = f"https://www.avito.ma/fr/{city}/{immobilier}-à_louer?price={min_price}-{max_price}-&size={min_size}-{max_size}"
    
                # visit the website with the url : my_url
                response = httpx.get(my_url, timeout=50.0, follow_redirects=True)
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
                # Initialize list to store URLs
                extracted_data = []

                main_div = market_soup.find('div', class_="sc-1nre5ec-1")
                #print(main_div)

                # Initialize list to store extracted data
                extracted_data = []

                main_div = market_soup.find('div', class_="sc-1nre5ec-1")
                if main_div:
                    for a_tag in main_div.find_all('a', class_="sc-1jge648-0"):
                        if 'href' in a_tag.attrs:
                            href_link = a_tag['href']
                            if not href_link.startswith("https://immoneuf.avito.ma"):
                                # Initialize a dictionary to store the current property details
                                property_details = {'URL': href_link, 'Salle de bain': None, 'Chambres': None, 'Surface totale': None}
                                # Loop through the sub-div elements to find relevant details
                                for sub_div in a_tag.find_all('div', class_="sc-b57yxx-2"):
                                    # Extract the text information within the span tag
                                    span_texts = sub_div.find_all('span', class_="sc-1s278lr-0")

                                    for span in span_texts:
                                        title = span.find('div', {'title': True})  # Look for 'div' with title attribute
                                        if title:
                                            title_text = title['title']
                                            value = span.find('span').text.strip()
                                                                                    # Map the title text to the correct field in the dictionary
                                            if 'Salle de bain' in title_text:
                                                property_details['Salle de bain'] = value
                                            elif 'Chambres' in title_text:
                                                property_details['Chambres'] = value
                                            elif 'Surface totale' in title_text:
                                                property_details['Surface totale'] = value

                                                                        # Check if all required fields are present
                                if all([property_details['URL'], property_details['Salle de bain'], 
                                        property_details['Chambres'], property_details['Surface totale']]):
                                    # Append the property details to the extracted data list
                                    extracted_data.append(property_details)

                i = 0            
                for link in [item['URL'] for item in extracted_data]:
                    
                    Salle_bain = extracted_data[i]['Salle de bain']
                    Salle_bain = int(Salle_bain)
                    Chambres = extracted_data[i]['Chambres']
                    Chambres = int(Chambres)
                    Surface_totale = extracted_data[i]['Surface totale']
                    Surface_totale = int(Surface_totale.replace('m²',''))
                    response = httpx.get(link, timeout=50.0)
                    time.sleep(2)

                    # Parse html
                    html = response.text
                    market_soup = soup(html,'html.parser')
                    
                    i = i + 1

                    # NOW I HAVE TO TAKE INFOS OF EXTRACTED_DATA AND PUT IT HERE. EACH LINK HAS ITS THREE OTHERS VARIABLES 
                    print('Salle de bain : ', Salle_bain)
                    print('Chambre : ', Chambres)
                    print('Surface_totale : ', Surface_totale)
                    title_element = market_soup.find('h1', class_='sc-1g3sn3w-12 jUtCZM')  # Adjust class names
                    title_element = title_element.text.strip()
                    
                    extras = []
                    items_plus = market_soup.find_all('div', class_='sc-mnh93t-2 gONgBt')
                    for item in items_plus:
                        label = item.find('span', class_='sc-1x0vz2r-0 bXFCIH').text.strip()
                        extras.append(label)
                    print('extras : ', extras)
                    # superficie
                    city = market_soup.find('span', class_='sc-1x0vz2r-0 iotEHk')
                    city = city.text.strip()
                    price_element = market_soup.find('p', class_='sc-1x0vz2r-0 lnEFFR sc-1g3sn3w-13 czygWQ')  # Adjust class names
                    price_element = price_element.text.strip()
                    price_element = price_element.replace(' DH','').replace('\u202f', '')
                    price_element = int(price_element)
                    print('price : ', price_element)
                    
                    globals()['Âge_du_bien'] = 'Missing'
                    globals()['Surface_habitable'] = 'Missing'
                    globals()['Salons'] = 'Missing'    
                    globals()['Étage'] = 'Missing'  
                    globals()["Nombre_d'étage"] = 'Missing'                      
                    
                    # Find all list items and extract the data
                    items = market_soup.find_all('li', class_='sc-qmn92k-1 jJjeGO')
                    for item in items:
                        label = item.find('span', class_='sc-1x0vz2r-0 jZyObG').text.strip()
                        label = label.replace(" ", "_")
                        value = item.find('span', class_='sc-1x0vz2r-0 gSLYtF').text.strip()
                        globals()[label] = value

                    # Check if either Étage or Nombre_etage is available
                    if globals().get("Nombre_d'étage") != 'Missing':
                        globals()['Étage'] = globals()["Nombre_d'étage"]

                    print(Salons)
                    print(Surface_habitable)


                    if (Salons == 'Missing') or (Surface_habitable == 'Missing') or (Étage == 'Missing'):
                        continue

                    etage = re.sub(r'[^\d]', '', Étage)
                    etage = int(etage)
                    print('etage : ', etage)
                    salons = int(Salons)
                    surface_habitable = int(Surface_habitable)
                    
                    # Split the string by the comma
                    split_variable = Type.split(',')

                    # Assign the first part to 'type' and clean up any extra spaces
                    Type_ = split_variable[0].strip()

                    # Assign the second part to 'transaction', remove 'à', and clean up any extra spaces
                    transaction = split_variable[1].replace('à', '').strip()

                    
                    print('Type : ', Type_)
                    print('transaction : ', transaction)
                    
                    print('Secteur : ' ,Secteur)
                    print('Salons : ',salons)
                    
                    print('Surface_habitable : ', Surface_habitable)


        
                    # working on extras
                    if 'Ascenseur' in extras :
                        ascenseur = 'Yes'
                    else:
                        ascenseur = 'No'
                    
                    if 'Sécurité' in extras :
                        securite = 'Yes'
                    else:
                        securite = 'No'
                        
                    if 'Concierge' in extras :
                        concierge = 'Yes'
                    else:
                        concierge = 'No'
                    
                    if 'Parking' in extras :
                        parking = 'Yes'
                    else:
                        parking = 'No'
                    
                    if 'Terrasse' in extras :
                        terrasse = 'Yes'
                    else:
                        terrasse = 'No'                        
                    
                    if 'Balcon' in extras :
                        balcon = 'Yes'
                    else:
                        balcon = 'No'  
                        
                    if 'Climatisation' in extras :
                        climatisation = 'Yes'
                    else :
                        climatisation = 'No'
                        
                    if 'Duplex' in extras:
                        duplex = 'Yes'
                    else:
                        duplex = 'No'
                        
                    if 'Chauffage' in extras:
                        chauffage = 'Yes'
                    else:
                        chauffage = 'No'
                        
                    if 'Meublé' in extras:
                        meuble = 'Yes'
                    else:
                        meuble = 'No'
                        
                    if 'Cuisine équipée' in extras:
                        cuisine_equipee = 'Yes'
                    else:
                        cuisine_equipee = 'No'

                    if 'Garage' in extras:
                        garage = 'Yes'
                    else:
                        garage = 'No'

                    if 'Jardin' in extras:
                        jardin = 'Yes'
                    else:
                        jardin = 'No'

                    if 'Piscine' in extras:
                        piscine = 'Yes'
                    else:
                        piscine = 'No'
                    
                        
                    print(ascenseur, securite, concierge, parking, terrasse, balcon, climatisation, 
                            duplex, chauffage, meuble, cuisine_equipee, garage, jardin, piscine)
                    
                    
                    print()
                    
                    new_row = {'titre' : title_element, 'Type' : Type_, 'transaction' : transaction, 'ville' : city, 'secteur' : Secteur, 'surface_totale' : Surface_totale, 
                                'surface_habitable' : Surface_habitable, 'chambres' : Chambres, 'salle_bains' : Salle_bain, 'salons' : salons,
                                'pieces' : salons+Chambres, 'etage' : etage, 'terrasse' : terrasse, 'balcon' : balcon, 'parking' : parking, 
                                'ascenseur' : ascenseur, 'securite' : securite, 'climatisation' : climatisation, 'cuisine_equipee' : cuisine_equipee,
                                'concierge' : concierge, 'duplex' : duplex, 'chauffage' : chauffage, 'meuble' : meuble, 
                                'garage' : garage, 'jardin' : jardin, 'piscine' : piscine,'prix' : price_element
                            }
                    
                    if 'Missing' not in new_row.values():
                        new_row_df = pd.DataFrame([new_row])
                        my_df = pd.concat([my_df, new_row_df], ignore_index=True)
                        
                        # Append to your main DataFrame here
                    else:
                        print("Row not appended due to 'Missing' values.")
                    
                    print(my_df)
                    
                logging.info('Extracting data for appartements and putting it in DataFrame format was successful')
            except Exception as e:
                logging.error(f'Error while Extracting Data for appartements and putting it in DataFrame format : {e}')
                raise e
            

    try:
        
        logging.info('Selecting right data with right city')
        my_df = my_df[my_df['ville'] == 'Casablanca']
        
        # Deleting Outliers will be done after. 

        # lower_quantile = my_df.groupby(['secteur','Type'])['prix'].quantile(0.05).reset_index()
        # upper_quantile = my_df.groupby(['secteur','Type'])['prix'].quantile(0.95).reset_index()
        # lower_quantile.rename(columns={'prix': '0.05_price'}, inplace=True)
        # upper_quantile.rename(columns={'prix': '0.95_price'}, inplace=True)
        # adding_2 = pd.merge(my_df , lower_quantile, on=['ville_secteur', 'Type'], how='left')
        # final_df = pd.merge(adding_2 , upper_quantile, on=['ville_secteur', 'Type'], how='left')
        # final_df = final_df[(final_df['prix']>final_df['0.05_price']) & (final_df['prix']<final_df['0.95_price'])]
        # final_df = final_df.iloc[:,:-2]

        # lower_quantile = final_df.groupby(['ville_secteur','Type'])['surface_totale'].quantile(0.05).reset_index()
        # upper_quantile = final_df.groupby(['ville_secteur','Type'])['surface_totale'].quantile(0.95).reset_index()
        # lower_quantile.rename(columns={'surface_totale': '0.05_surface_totale'}, inplace=True)
        # upper_quantile.rename(columns={'surface_totale': '0.95_surface_totale'}, inplace=True)
        # adding_2 = pd.merge(my_df , lower_quantile, on=['ville_secteur', 'Type'], how='left')
        # final_df_2 = pd.merge(adding_2 , upper_quantile, on=['ville_secteur', 'Type'], how='left')
        # final_df_2 = final_df_2[(final_df_2['surface_totale']>final_df_2['0.05_surface_totale']) & (final_df_2['surface_totale']<final_df_2['0.95_surface_totale'])]
        # final_df_2 = final_df_2.iloc[:,:-2]
        my_df.to_csv('/opt/airflow/dags/scraped_data_rent_avito.csv',index=False)
        logging.info('Saving data in csv format was successful')

    except Exception as e:
        logging.error(f'Error while saving data into csv format : {e}')
    return 'Scraping and processing & staging data steps were completed'
