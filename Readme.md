# FHIR Data Pipeline

This project transforms healthcare CSV data into FHIR-compliant JSON using Python and Airflow.

## How to Run
1. Make sure Docker and Airflow are set up.
2. Place input files under `/opt/airflow/data/input`.
3. Run the Airflow DAG to generate FHIR resources.

## Output
FHIR JSON is saved under `/opt/airflow/data/output/fhir_output.json`.

## Tech Stack
- Python
- Apache Airflow
- fhir.resources
- Pandas
