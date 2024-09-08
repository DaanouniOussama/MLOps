import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table_mubawab():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/scraped_data_sell_mubawab.csv', "r") as file:
            cur.copy_expert(
                "COPY Real_Estate_table_mubawab_vente(Title , Real_estate_type , Ville , Secteur , Surface_totale , Chambres , Salle_bains , Pieces , Etat , Age_bien , Etage , Standing , Terrasse , Balcon, Parking , Ascenseur , Securite , Climatisation , Cuisine_equipee , Concierge , Chauffage , Garage, Jardin, Piscine, Salon_marocain , Salon_euro , Price) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL '' " ,
                file,
            )
            conn.commit()

        logging.info('Ingesting data into Postgres table was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into Postgres table : {e}')
        raise e
    
