import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_process_to_feature_store():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/processed_scraped.csv', "r") as file:
            cur.copy_expert(
                "COPY feature_store(real_estate_type, superficie, rooms, bath_room, floor, age, neighbourhood_city_coded, neighbourhood_city, city, price) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL '' " ,
                file,
            )
            conn.commit()

        logging.info('Ingesting data into feature store was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into feature store : {e}')
        raise e
    
