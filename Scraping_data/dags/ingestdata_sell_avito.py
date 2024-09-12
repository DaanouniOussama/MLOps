import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table():
    try:
        # Establish connection to Postgres
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Create a temporary table for the imported data
        cur.execute("""
            CREATE TEMP TABLE temp_sell_data AS 
            SELECT * FROM Real_Estate_table_avito_vente LIMIT 0;
        """)

        # Copy CSV data to the temporary table
        csv_file_path = '/opt/airflow/dags/scraped_data_sell_avito.csv'
        with open(csv_file_path, "r") as file:
            cur.copy_expert(
                """
                COPY temp_sell_data(Title, Real_estate_type, Transaction, Ville, Secteur, Surface_totale, 
                Surface_habitable, Chambres, Salle_bains, Salons, Pieces, Etage, Age_bien, Terrasse, Balcon, 
                Parking, Ascenseur, Securite, Climatisation, Cuisine_equipee, Concierge, Duplex, Chauffage, 
                Meuble, Garage, Jardin, Piscine, Price)
                FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL ''
                """, 
                file,
            )

        # Insert non-duplicate rows from the temp table into the main table
        cur.execute("""
            INSERT INTO Real_Estate_table_avito_vente (
                Title, Real_estate_type, Transaction, Ville, Secteur, Surface_totale, Surface_habitable, 
                Chambres, Salle_bains, Salons, Pieces, Etage, Age_bien, Terrasse, Balcon, Parking, Ascenseur, 
                Securite, Climatisation, Cuisine_equipee, Concierge, Duplex, Chauffage, Meuble, Garage, 
                Jardin, Piscine, Price
            )
            SELECT temp.Title, temp.Real_estate_type, temp.Transaction, temp.Ville, temp.Secteur, 
                temp.Surface_totale, temp.Surface_habitable, temp.Chambres, temp.Salle_bains, 
                temp.Salons, temp.Pieces, temp.Etage, temp.Age_bien,temp.Terrasse, temp.Balcon, temp.Parking, 
                temp.Ascenseur, temp.Securite, temp.Climatisation, temp.Cuisine_equipee, 
                temp.Concierge, temp.Duplex, temp.Chauffage, temp.Meuble, temp.Garage, temp.Jardin, 
                temp.Piscine, temp.Price
            FROM temp_sell_data temp
            WHERE NOT EXISTS (
                SELECT 1 FROM Real_Estate_table_avito_vente main
                WHERE main.Title = temp.Title
                AND main.Real_estate_type = temp.Real_estate_type
                AND main.Ville = temp.Ville
                AND main.Secteur = temp.Secteur
                AND main.Surface_totale = temp.Surface_totale
                AND main.Price = temp.Price
            );
        """)

        conn.commit()

        # Now, let's remove the already processed rows from the CSV
        with open(csv_file_path, "r") as infile, open(csv_file_path + ".tmp", "w", newline='') as outfile:
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
                    AND Surface_totale = %s
                    AND Price = %s
                """, (row['titre'], row['Type'], row['ville'], row['secteur'], row['surface_totale'], row['prix']))

                if cur.fetchone() is None:
                    # Write rows that are not in the DB back to the new CSV file
                    writer.writerow(row)

        # Replace the original file with the updated one
        import os
        os.replace(csv_file_path + ".tmp", csv_file_path)

        logging.info('Ingesting data into Postgres table and cleaning CSV was successful')

    except Exception as e:
        logging.error(f'Error while processing data: {e}')
        raise e

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
