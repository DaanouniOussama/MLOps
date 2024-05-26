import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_db")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/processed_scraped.csv', "r") as file:
            cur.copy_expert(
                "COPY Real_estate(title, Real_estate_type, Price, Superficie, floor, City) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
            conn.commit()

        logging.info('Ingesting data into Postgres table was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into Postgres table : {e}')
        raise e

    
    
    
