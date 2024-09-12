import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

def copy_csv_to_table_mubawab_rent():
    try:
        # Establish connection to Postgres
        postgres_hook = PostgresHook(postgres_conn_id="real_estate_connexion")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Create a temporary table for the imported data
        cur.execute("""
            CREATE TEMP TABLE temp_rent_data_mubawab AS 
            SELECT * FROM Real_Estate_table_mubawab_rent LIMIT 0;
        """)

        # Copy CSV data to the temporary table
        csv_file_path = '/opt/airflow/dags/scraped_data_rent_mubawab.csv'
        with open(csv_file_path, "r") as file:
            cur.copy_expert(
                """
                COPY temp_rent_data_mubawab(Title, Real_estate_type, Ville, Secteur, Surface_totale, 
                Chambres, Salle_bains, Pieces, Etat, Age_bien, Etage, Standing, Terrasse, Balcon, 
                Parking, Ascenseur, Securite, Climatisation, Cuisine_equipee, Concierge, Chauffage, 
                Garage, Jardin, Piscine, Salon_marocain, Salon_euro, Price)
                FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' NULL ''
                """, 
                file,
            )

        # Insert non-duplicate rows from the temp table into the main table
        cur.execute("""
            INSERT INTO Real_Estate_table_mubawab_rent (
                Title, Real_estate_type, Ville, Secteur, Surface_totale, Chambres, Salle_bains, Pieces, 
                Etat, Age_bien, Etage, Standing, Terrasse, Balcon, Parking, Ascenseur, Securite, 
                Climatisation, Cuisine_equipee, Concierge, Chauffage, Garage, Jardin, Piscine, 
                Salon_marocain, Salon_euro, Price
            )
            SELECT temp.Title, temp.Real_estate_type, temp.Ville, temp.Secteur, temp.Surface_totale, 
                temp.Chambres, temp.Salle_bains, temp.Pieces, temp.Etat, temp.Age_bien, temp.Etage, 
                temp.Standing, temp.Terrasse, temp.Balcon, temp.Parking, temp.Ascenseur, 
                temp.Securite, temp.Climatisation, temp.Cuisine_equipee, temp.Concierge, temp.Chauffage, 
                temp.Garage, temp.Jardin, temp.Piscine, temp.Salon_marocain, temp.Salon_euro, temp.Price
            FROM temp_rent_data_mubawab temp
            WHERE NOT EXISTS (
                SELECT 1 FROM Real_Estate_table_mubawab_rent main
                WHERE main.Title = temp.Title
                AND main.Real_estate_type = temp.Real_estate_type
                AND main.Ville = temp.Ville
                AND main.Secteur = temp.Secteur
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
                    SELECT 1 FROM Real_Estate_table_mubawab_rent
                    WHERE Title = %s
                    AND Real_estate_type = %s
                    AND Ville = %s
                    AND Secteur = %s
                    AND Price = %s
                """, (row['titre'], row['Type'], row['ville'], row['secteur'], row['prix']))

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
