import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/scraped_data_sell_avito.csv', "r") as file:
            cur.copy_expert(
                "COPY Real_Estate_table_avito_vente(Title , Real_estate_type , Transaction, Ville , Secteur , Surface_totale , Surface_habitable , Chambres , Salle_bains , Salons , Pieces , Age_bien , Terrasse , Balcon, Parking , Ascenseur , Securite , Climatisation , Cuisine_equipee , Concierge , Duplex , Chauffage , Meuble , Garage, Jardin, Piscine, Price) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL '' " ,
                file,
            )
            conn.commit()

        logging.info('Ingesting data into Postgres table was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into Postgres table : {e}')
        raise e

    
    
    
