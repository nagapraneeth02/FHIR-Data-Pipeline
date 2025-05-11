import sys
import os   
sys.path.append('/opt/airflow/scripts')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.transform_csv_to_fhir import csv_to_fhir
from scripts.transform_csv_to_fhir import csv_to_fhir
from scripts.post_to_hapi import post_to_hapi

default_args = {
    'start_date': datetime(2025, 5, 1),
}

with DAG(
    dag_id='csv_to_fhir_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    transform_task = PythonOperator(
    task_id='transform_csv_to_fhir',
    python_callable=csv_to_fhir,
    op_kwargs={
        'input_path': '/opt/airflow/data/input/healthcare_data.csv',
        'output_path': '/opt/airflow/data/output/fhir_output.json'
    }
)

    # Post to HAPI FHIR server
    post_task = PythonOperator(
        task_id='post_fhir_to_hapi',
        python_callable=post_to_hapi,
        op_kwargs={
            'fhir_file_path': '/opt/airflow/data/output/fhir_output.json',
            'hapi_url': 'http://localhost:3000/fhir'  # Use "hapi-fhir:8080" if inside Docker network
        }
    )
    
    
    transform_task >> post_task
    
