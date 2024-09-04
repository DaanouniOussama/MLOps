import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_process_to_feature_store_rent():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/processed_scraped_avito_rent.csv', "r") as file:
            cur.copy_expert(
                "COPY feature_store_rent_avito(Real_estate_type,Transaction,Ville,ville_secteur,Surface_totale,Surface_habitable,Chambres,Salle_bains,Salons,Pieces,Terrasse,Balcon,Parking,Ascenseur,Securite,Climatisation,Cuisine_equipee,Concierge,Duplex,Chauffage,Meuble, Garage , Jardin  , Piscine , Prix, Ville_secteur_coded) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL '' " ,
                file,
            )
            conn.commit()

        logging.info('Ingesting data into feature store was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into feature store : {e}')
        raise e
    
