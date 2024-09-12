import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table():
    try:
        # Establish connection to Postgres
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # File paths
        csv_file_path = '/opt/airflow/dags/scraped_data_sell_avito.csv'
        temp_csv_file_path = csv_file_path + ".tmp"

        # Read the CSV and check for duplicates
        with open(csv_file_path, "r") as infile, open(temp_csv_file_path, "w", newline='') as outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                # Check if the row already exists in the database
                cur.execute("""
                    SELECT 1 FROM Real_Estate_table_avito_vente
                    WHERE Title = %s
                    AND Real_estate_type = %s
                    AND Ville = %s
                    AND Secteur = %s
                    AND Price = %s
                """, (row['titre'], row['Type'], row['ville'], row['secteur'], row['prix']))

                if cur.fetchone() is None:
                    # Write rows that are not duplicates to the new CSV file
                    writer.writerow(row)

        # Replace the original file with the updated one (without duplicates)
        import os
        os.replace(temp_csv_file_path, csv_file_path)

        # Now ingest the cleaned CSV data into the table
        with open(csv_file_path, "r") as file:
            cur.copy_expert(
                """
                COPY Real_Estate_table_avito_vente(Title , Real_estate_type , Transaction, Ville , 
                Secteur , Surface_totale , Surface_habitable , Chambres , Salle_bains , Salons , Pieces , 
                Etage, Age_bien, Terrasse , Balcon, Parking , Ascenseur , Securite , Climatisation , Cuisine_equipee , 
                Concierge , Duplex , Chauffage , Meuble , Garage, Jardin, Piscine, Price)
                FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL ''
                """, 
                file,
            )

        conn.commit()

        logging.info('Data ingestion and CSV cleaning was successful')

    except Exception as e:
        logging.error(f'Error while processing data: {e}')
        raise e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()