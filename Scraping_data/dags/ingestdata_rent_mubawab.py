import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table_mubawab_rent():
    try:
        # Establish connection to Postgres
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # File paths
        csv_file_path = '/opt/airflow/dags/scraped_data_rent_mubawab.csv'
        temp_csv_file_path = csv_file_path + ".tmp"

        # Read the CSV and check for duplicates
        with open(csv_file_path, "r") as infile, open(temp_csv_file_path, "w", newline='') as outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                # Check if the row already exists in the database
                cur.execute("""
                    SELECT 1 FROM Real_Estate_table_mubawab_rent
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
                COPY Real_Estate_table_mubawab_rent(Title, Real_estate_type, Ville, Secteur, 
                Surface_totale, Chambres, Salle_bains, Pieces, Etat, Age_bien, Etage, Standing, 
                Terrasse, Balcon, Parking, Ascenseur, Securite, Climatisation, Cuisine_equipee, 
                Concierge, Chauffage, Garage, Jardin, Piscine, Salon_marocain, Salon_euro, Price)
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
