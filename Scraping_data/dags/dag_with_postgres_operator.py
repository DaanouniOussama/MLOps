# Importing libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator  
from airflow.operators.python_operator import PythonOperator
from scraper import scrapper
from ingestdata import copy_csv_to_table


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id = 'dag_postgres_operator', default_args=default_args, schedule_interval=timedelta(days=1)) as dag :

    Connexion = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_db',
        sql ="""
            CREATE TABLE IF NOT EXISTS Real_estate (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                Real_estate_type VARCHAR(30),
                Price INT,
                Superficie INT,
                Floor INT,
                City VARCHAR(30)
            );
        """
    )

    scrape_task = PythonOperator(
        task_id='scrape_website',
        python_callable=scrapper,
    )

    # Define the task
    ingest_task = PythonOperator(
        task_id='Copy-csv-to-table',
        python_callable=copy_csv_to_table,
    )


    Connexion >> scrape_task >> ingest_task
   