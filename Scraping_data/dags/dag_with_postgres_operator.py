# Importing libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator  
from airflow.operators.python_operator import PythonOperator
from scraper_sell_avito import scrapper
from ingestdata_sell_avito import copy_csv_to_table
from lalt_long_calculation_sell_avito import lati_long
from ingesting_maps_data_sell_avito import copy_csv_to_table_maps
from feature_engineering_sell_avito import preprocess
from Ingest_feature_store_sell_avito import copy_process_to_feature_store
from scraper_rent_avito import scrapper_rent
from ingestdata_rent_avito import copy_csv_to_table_rent
from lalt_long_calculation_rent_avito import lati_long_rent
from ingesting_maps_data_rent_avito import copy_csv_to_table_maps_rent
from feature_engineering_rent_avito import preprocess_rent
from ingest_feature_store_rent_avito import copy_process_to_feature_store_rent


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id = 'dag_data_engineering_avito_sell', default_args=default_args, schedule_interval=timedelta(days=30)) as dag :

    Connexion = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS Real_Estate_table_avito_vente (
                id SERIAL PRIMARY KEY,
                Title VARCHAR(255),
                Real_estate_type VARCHAR(30),
                Transaction VARCHAR(30),
                Ville VARCHAR(30),
                Secteur VARCHAR(30),
                Surface_totale INT,
                Surface_habitable INT,
                Chambres INT,
                Salle_bains INT,
                Salons INT,
                Pieces INT,
                Age_bien VARCHAR(30),
                Terrasse VARCHAR(5),
                Balcon VARCHAR(5),
                Parking VARCHAR(5),
                Ascenseur VARCHAR(5),
                Securite VARCHAR(5),
                Climatisation VARCHAR(5),
                Cuisine_equipee VARCHAR(5),
                Concierge VARCHAR(5),
                Duplex VARCHAR(5),
                Chauffage VARCHAR(5),
                Meuble VARCHAR(5),
                Garage VARCHAR(5),
                Jardin VARCHAR(5),
                Piscine VARCHAR(5),
                Price INT,
                unix_time BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
            );
        """
    )

    scrape_task = PythonOperator(
        task_id='scrape_website',
        python_callable=scrapper,
    )

    # Ingest scraped data
    ingest_task = PythonOperator(
        task_id='Copy-csv-to-table',
        python_callable=copy_csv_to_table,
    )


    # Create table for feature store
    create_table_feature_store_task = PostgresOperator(
        task_id = 'create_feature_store',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS feature_store_vente_avito (
                id SERIAL PRIMARY KEY,
                Real_estate_type INT,
                Transaction VARCHAR(30),
                Ville INT,
                ville_secteur VARCHAR(50),
                Surface_totale INT,
                Surface_habitable INT,
                Chambres INT,
                Salle_bains INT,
                Salons INT,
                Pieces INT,
                Age_bien INT,
                Terrasse INT,
                Balcon INT,
                Parking INT,
                Ascenseur INT,
                Securite INT,
                Climatisation INT,
                Cuisine_equipee INT,
                Concierge INT,
                Duplex INT,
                Chauffage INT,
                Meuble INT,
                Garage INT,
                Jardin INT,
                Piscine INT,
                Prix INT,
                Ville_secteur_coded INT
            );
        """
    )

    # Process the scraped data (The Feature Store)
    feature_engineering_task = PythonOperator(
        task_id='feature_engineering',
        python_callable=preprocess,
    )

    # Ingest data, feature store table creation
    copy_process_to_feature_store_task = PythonOperator(
        task_id='feature_engineering_to_feature_store',
        python_callable=copy_process_to_feature_store,
    )

    create_table_maps_connexion = PostgresOperator(
        task_id = 'create_postgres_table_maps',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS maps_table_avito_sell (
                id SERIAL PRIMARY KEY,
                neighbourhood_city VARCHAR(100),
                longitude FLOAT,
                laltitude FLOAT
            );
        """
    )

    long_lalt_calculation = PythonOperator(
        task_id='latitude_longitude_calculation',
        python_callable=lati_long,
    )

    ingestion = PythonOperator(
        task_id='ingestion',
        python_callable=copy_csv_to_table_maps,
    )


    Connexion >> scrape_task >> ingest_task >> create_table_feature_store_task >> feature_engineering_task >> copy_process_to_feature_store_task >> create_table_maps_connexion >> long_lalt_calculation >> ingestion



with DAG(dag_id = 'dag_data_engineering_avito_rent', default_args=default_args, schedule_interval=timedelta(days=30)) as dag :

    Connexion = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS Real_Estate_table_avito_location (
                id SERIAL PRIMARY KEY,
                Title VARCHAR(255),
                Real_estate_type VARCHAR(30),
                Transaction VARCHAR(30),
                Ville VARCHAR(30),
                Secteur VARCHAR(30),
                Surface_totale INT,
                Surface_habitable INT,
                Chambres INT,
                Salle_bains INT,
                Salons INT,
                Pieces INT,
                Terrasse VARCHAR(5),
                Balcon VARCHAR(5),
                Parking VARCHAR(5),
                Ascenseur VARCHAR(5),
                Securite VARCHAR(5),
                Climatisation VARCHAR(5),
                Cuisine_equipee VARCHAR(5),
                Concierge VARCHAR(5),
                Duplex VARCHAR(5),
                Chauffage VARCHAR(5),
                Meuble VARCHAR(5),
                Garage VARCHAR(5),
                Jardin VARCHAR(5),
                Piscine VARCHAR(5),
                Price INT,
                unix_time BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
            );
        """
    )

    scrape_task = PythonOperator(
        task_id='scrape_website',
        python_callable=scrapper_rent,
    )

    # Ingest scraped data
    ingest_task = PythonOperator(
        task_id='Copy-csv-to-table',
        python_callable=copy_csv_to_table_rent,
    )


    # Create table for feature store
    create_table_feature_store_task = PostgresOperator(
        task_id = 'create_feature_store',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS feature_store_rent_avito (
                id SERIAL PRIMARY KEY,
                Real_estate_type INT,
                Transaction VARCHAR(30),
                Ville INT,
                ville_secteur VARCHAR(50),
                Surface_totale INT,
                Surface_habitable INT,
                Chambres INT,
                Salle_bains INT,
                Salons INT,
                Pieces INT,
                Terrasse INT,
                Balcon INT,
                Parking INT,
                Ascenseur INT,
                Securite INT,
                Climatisation INT,
                Cuisine_equipee INT,
                Concierge INT,
                Duplex INT,
                Chauffage INT,
                Meuble INT,
                Garage INT,
                Jardin INT,
                Piscine INT,
                Prix INT,
                Ville_secteur_coded INT
            );
        """
    )

    # Process the scraped data (The Feature Store)
    feature_engineering_task = PythonOperator(
        task_id='feature_engineering',
        python_callable=preprocess_rent,
    )

    # Ingest data, feature store table creation
    copy_process_to_feature_store_task = PythonOperator(
        task_id='feature_engineering_to_feature_store',
        python_callable=copy_process_to_feature_store_rent,
    )

    create_table_maps_connexion = PostgresOperator(
        task_id = 'create_postgres_table_maps',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS maps_table_avito_rent (
                id SERIAL PRIMARY KEY,
                neighbourhood_city VARCHAR(100),
                longitude FLOAT,
                laltitude FLOAT
            );
        """
    )

    long_lalt_calculation = PythonOperator(
        task_id='latitude_longitude_calculation',
        python_callable=lati_long_rent,
    )

    ingestion = PythonOperator(
        task_id='ingestion',
        python_callable=copy_csv_to_table_maps_rent,
    )


    Connexion >> scrape_task >> ingest_task >> create_table_feature_store_task >> feature_engineering_task >> copy_process_to_feature_store_task >> create_table_maps_connexion >> long_lalt_calculation >> ingestion
 

