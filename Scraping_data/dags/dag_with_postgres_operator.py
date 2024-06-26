# Importing libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator  
from airflow.operators.python_operator import PythonOperator
from scraper import scrapper
from ingestdata import copy_csv_to_table
from lalt_long_calculation import lati_long
from ingesting_maps_data import copy_csv_to_table_maps


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id = 'dag_data_engineering_', default_args=default_args, schedule_interval=timedelta(days=30)) as dag :

    Connexion = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS Real_Estate_table (
                id SERIAL PRIMARY KEY,
                Title VARCHAR(255),
                Real_estate_type VARCHAR(30),
                Price INT,
                Superficie INT,
                Rooms INT,
                Bath_room INT,
                Floor INT,
                Age VARCHAR(30),
                neighbourhood VARCHAR(100),
                City VARCHAR(30),
                unix_time BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
            );
        """
    )

    scrape_task = PythonOperator(
        task_id='scrape_website',
        python_callable=scrapper,
        op_args = ['casablanca']
    )

    # Define the task
    ingest_task = PythonOperator(
        task_id='Copy-csv-to-table',
        python_callable=copy_csv_to_table,
    )


    create_table_maps_connexion = PostgresOperator(
        task_id = 'create_postgres_table_maps',
        postgres_conn_id = 'real_estate_connexion',
        sql ="""
            CREATE TABLE IF NOT EXISTS maps_table (
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


    Connexion >> scrape_task >> ingest_task >> create_table_maps_connexion >> long_lalt_calculation >> ingestion


   
   

