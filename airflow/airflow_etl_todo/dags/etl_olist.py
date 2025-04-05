from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_data
from scripts.load import load_data
from scripts.transform import transform_data
from scripts.cleaning import cleaning_data

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_olist',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Ejecuta el DAG diariamente
    catchup=False,
    tags=['ETL', 'CSV', 'sqlite'],
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    clean = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # Definición de dependencias en el orden solicitado
    extract >> load >> clean >> transform