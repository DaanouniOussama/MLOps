
# Importing libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator  
from airflow.operators.python_operator import PythonOperator
from lalt_long_calculation import lati_long
from ingesting_maps_data import copy_csv_to_table 
import geopy


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id = 'data_engineering_maps', default_args=default_args, schedule_interval=timedelta(days=10)) as dag :


    create_table_maps_connexion = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_db',
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
        task_id='latitude and longitude calculation',
        python_callable=lati_long,
    )

    ingestion = PythonOperator(
        task_id='ingestion',
        python_callable=copy_csv_to_table,
    )


create_maps_table_connexion >> long_lalt_calculation >> ingestion