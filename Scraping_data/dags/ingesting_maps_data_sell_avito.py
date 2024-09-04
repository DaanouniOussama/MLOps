import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table_maps():
    try:
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open('/opt/airflow/dags/long_lalt_sell_avito.csv', "r") as file:
            cur.copy_expert(
                "COPY maps_table_avito_sell(neighbourhood_city, laltitude, longitude) FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"' NULL '' " ,
                file,
            )
            conn.commit()

        logging.info('Ingesting data into Postgres table was successful')

    except Exception as e:
        logging.error(f'Error while Ingesting data into Postgres table : {e}')
        raise e