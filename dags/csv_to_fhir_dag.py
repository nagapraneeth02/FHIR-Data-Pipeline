import sys
sys.path.append('/opt/airflow/scripts')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.transform_csv_to_fhir import csv_to_fhir

with DAG(
    dag_id='csv_to_fhir_pipeline',
    start_date=datetime(2025, 4, 16),
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

    transform_task