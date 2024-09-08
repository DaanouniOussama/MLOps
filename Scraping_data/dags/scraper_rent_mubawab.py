import logging
from bs4 import BeautifulSoup as soup
import pandas as pd
import httpx
import time
import re


def scrapper_mubawab_location():

    real_estate = ['appartements', 'villas-et-maisons-de-luxe']

    # My DataFrame
    my_df = pd.DataFrame(columns=['titre', 'Type', 'ville', 'secteur', 'surface_totale', 
                                   'chambres', 'salle_bains' ,
                                  'pieces', 'etat', 'age_bien', 'etage',
                                'standing', 'terrasse', 'balcon', 'parking', 
                                  'ascenseur', 'securite', 'climatisation', 'cuisine_equipee',
                                 'concierge', 'chauffage',
                               'garage', 'jardin', 'piscine',
                               'salon_marocain', 'salon_euro', 'prix'])
    # Initialize variables to store values
    
    
    for ville in ['casablanca']:
        for immobilier in real_estate:
            
            
            # URL : https://www.mubawab.ma/fr/st/casablanca/appartements-a-vendre:prmn:200000
            my_url = f"https://www.mubawab.ma/fr/st/{ville.lower()}/{immobilier}-a-louer:prmn:2000"

            # visit the website with the url : my_url
            response = httpx.get(my_url, timeout=10.0, follow_redirects=True)
            time.sleep(2)

            # Parse html
            html = response.text
            market_soup = soup(html,'html.parser')
            # Initialize list to store URLs
            extracted_data = []

            main_div = market_soup.find('ul', class_="ulListing")
            #print(main_div)

            # Initialize list to store extracted data
            extracted_data = []

            if main_div:
                for a_tag in main_div.find_all('li', class_="listingBox w100"):
                    if 'linkref' in a_tag.attrs:
                        href_link = a_tag['linkref']
                        
                        # Initialize a dictionary to store the current property details
                        property_details = {'URL': href_link}
        
                        extracted_data.append(property_details)
            
            print(extracted_data)

            i = 0            
            for link in [item['URL'] for item in extracted_data]:
                
                response = httpx.get(link, timeout=10.0, follow_redirects=True)
                time.sleep(2)

                # Parse html
                html = response.text
                market_soup = soup(html,'html.parser')
                
                i = i + 1
                
                try:
                    logging.info('Extrating title, ville and secteur ...')
                    title = market_soup.find('h1' , class_ = 'searchTitle').text.strip()
                    print(title)
                    
                    ville_secteur = market_soup.find('h3', class_='greyTit').text.strip()

                    # Split the text into 'secteur' and 'ville'
                    secteur = ville_secteur.split('à')[0].strip()
                    ville = ville_secteur.split('à')[1].strip()
                    print(f'ville : {ville}')
                    print(f'secteur : {secteur}')
                    

                except Exception as e:
                    logging.error('Error while extracting ville et secteur')
                    raise e
                    
                try:  

                    logging.info('Extrating price ...')
                    # NOW I HAVE TO TAKE INFOS OF EXTRACTED_DATA AND PUT IT HERE. EACH LINK HAS ITS THREE OTHERS VARIABLES 
                    price_element = market_soup.find('h3', class_='orangeTit')  # Adjust class names
                    price_element = price_element.text.strip()  
                    price_element = price_element.replace(' DH','').replace('\xa0', '').strip()
                    # Use a regular expression to extract the first sequence of digits
                    price_numeric = re.search(r'\d+', price_element)
                    # Check if a numeric value was found, then convert it to an integer
                    if price_numeric:
                        price_element = int(price_numeric.group())
                        print('price : ', price_element)
                    else:
                        print('No valid price found.')
                    
                    
                except Exception as e:
                    logging.error('Error while extracting price')
                    raise e
                    
                try:
                    logging.info('Extrating surface, pieces, chambres, salles_bains ...')

                    # Extract surface, pieces, chambres et salles de bains
                    # Use default value (None or 0) if element is not found
                    surface = market_soup.find('i', class_='icon-triangle')
                    surface = int(surface.find_next_sibling('span').text.strip().split()[0]) if surface else None

                    pieces = market_soup.find('i', class_='icon-house-boxes')
                    pieces = int(pieces.find_next_sibling('span').text.strip().split()[0]) if pieces else None

                    chambres = market_soup.find('i', class_='icon-bed')
                    chambres = int(chambres.find_next_sibling('span').text.strip().split()[0]) if chambres else None

                    salles_bains = market_soup.find('i', class_='icon-bath')
                    salles_bains = int(salles_bains.find_next_sibling('span').text.strip().split()[0]) if salles_bains else None

                    print(f'surface : {surface}, pieces : {pieces} , chambres : {chambres} , salles de bains : {salles_bains}')
                    
                except Exception as e:
                    logging.error('Error while extracting surface, pieces, chambres, salles de bains')
                    raise e
                
                try:
                    # Find all the feature blocks
                    features = market_soup.find_all('div', class_='adMainFeature')

                    # Initialize variables to None
                    type_bien, etat, etat_bien, etage_bien, etage_bien_numeric, standing  = None, None, None, None, 'Not_defined', 'Not_defined'

                    # Iterate through the features to extract the values
                    for feature in features:
                        label = feature.find('p', class_='adMainFeatureContentLabel').text.strip()
                        value = feature.find('p', class_='adMainFeatureContentValue').text.strip()

                        # Assign values based on the label
                        if label == 'Type de bien':
                            type_bien = value
                        elif label == 'Etat':
                            etat = value
                        elif label == 'Etat du bien':
                            etat_bien = value
                        elif label == 'Standing':
                            standing = value
                        elif label == 'Étage du bien':
                            etage_bien = value
                            etage_bien_numeric = int(''.join(filter(str.isdigit, etage_bien)))
                            
                            

                    # Print the extracted values stored in variables
                    print(f'Type de bien: {type_bien}')
                    print(f'Etat: {etat}')
                    print(f'Etat du bien: {etat_bien}')
                    print(f'standing: {standing}')
                    print(f'etage_bien: {etage_bien_numeric}')
                
                except Exception as e:
                    logging.error('Error while extracting main features')
                    raise e
                    
                try: 
                    extras = []

                    # Find all 'adFeature' divs
                    features = market_soup.find_all('div', class_='adFeature')

                    # Iterate through the features and extract the text from 'span'
                    for feature in features:
                        label = feature.find('span', class_='fSize11 centered').text.strip()
                        extras.append(label)

                    # Print the list of extras
                    print(extras)

                except Exception as e:
                    logging.error('Error while extracting extra features')
                    raise e  
                try:    
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

                    if 'Chauffage central' in extras:
                        chauffage = 'Yes'
                    else:
                        chauffage = 'No'

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

                    if 'Salon Marocain' in extras:
                        salon_marocain = 'Yes'
                    else:
                        salon_marocain = 'No'

                    if 'Salon européen' in extras:
                        salon_euro = 'Yes'
                    else:
                        salon_euro = 'No'


                    print(ascenseur, securite, concierge, parking, terrasse, balcon, 
                          climatisation, chauffage, cuisine_equipee,salon_marocain, salon_euro)


                    print()

                    new_row = {'titre' : title, 'Type' : type_bien, 'ville' : ville, 'secteur' : secteur, 'surface_totale' : surface, 
                                   'chambres' : chambres, 'salle_bains' : salles_bains,
                                  'pieces' : pieces, 'etat' : etat, 'age_bien' : etat_bien, 'etage' : etage_bien_numeric,
                                'standing' : standing, 'terrasse' : terrasse, 'balcon' : balcon, 'parking' : parking, 
                                  'ascenseur' : ascenseur, 'securite' : securite, 'climatisation' : climatisation, 'cuisine_equipee' : cuisine_equipee,
                                 'concierge' : concierge, 'chauffage' : chauffage,
                               'garage' : garage, 'jardin' : jardin, 'piscine' : piscine,
                               'salon_marocain' : salon_marocain, 'salon_euro' : salon_euro, 'prix' : price_element
                              }

                    print(new_row)
                    if None not in new_row.values():
                        new_row_df = pd.DataFrame([new_row])
                        my_df = pd.concat([my_df, new_row_df], ignore_index=True)
                        # Append to your main DataFrame here
                    else:
                        print("Row not appended due to 'Missing' values.")

                    print(my_df)

                    logging.info('Extracting data and putting it in DataFrame format was successful')
                except Exception as e:
                    logging.error(f'Error while Extracting Data and putting it in DataFrame format : {e}')
                    raise e
    try:
        
        logging.info('Selecting right data with right city')
        my_df = my_df[my_df['ville'] == 'Casablanca']
        my_df.to_csv('/opt/airflow/dags/scraped_data_rent_mubawab.csv',index=False)
        logging.info('Saving data in csv format was successful')

    except Exception as e:
        logging.error(f'Error while saving data into csv format : {e}')
    return 'Scraping and processing & staging data steps were completed'