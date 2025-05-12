# 🏥 Healthcare Data Pipeline: CSV to FHIR JSON to HAPI FHIR Server

This project demonstrates a complete healthcare data transformation pipeline using Python. It takes a dataset in CSV format, converts it into [FHIR](https://www.hl7.org/fhir/) (Fast Healthcare Interoperability Resources) compliant JSON, and uploads it to a [HAPI FHIR](https://hapifhir.io/) server running locally or in a cloud environment (e.g., Google Cloud Shell).

---

## 🚀 Project Overview

- 📄 **Input**: Clinical data in CSV format (e.g., patients, encounters)
- 🔄 **Transform**: Converts the CSV to FHIR-compliant JSON format
- ☁️ **Load**: Uploads the resulting JSON to a running HAPI FHIR server via REST API
- ✅ **Result**: Data can be queried using the FHIR interface

---

## 🧾 Description

This project demonstrates how to build a healthcare data pipeline that standardizes flat CSV files into structured FHIR (Fast Healthcare Interoperability Resources) format. The transformed JSON is then uploaded to a HAPI FHIR server. This mimics a common task in clinical informatics — converting legacy data into modern, interoperable formats.

It is especially useful for developers and health informatics professionals working with HL7 FHIR, EHR integration, or prototyping clinical data ingestion workflows.

---

## 🛠️ Tech Stack

- **Python 3**
- **HAPI FHIR Server** (Docker)
- **Google Cloud Shell** (optional)
- **Libraries**: `requests`, `json`, `os`

---

## ▶️ How to Run This Project

This project is built using Apache Airflow to orchestrate a healthcare data pipeline that reads clinical data from a CSV file, transforms it into FHIR-compliant JSON, and uploads it to a HAPI FHIR server. The workflow is managed using a DAG (`csv_to_fhir_dag.py`) that includes two main tasks: the first uses a Python script (`transform_csv_to_fhir.py`) to read a CSV file and convert each record into a valid FHIR resource in JSON format, while the second task uses `post_to_hapi.py` to post each resource to the HAPI FHIR server. 

To run the project, Airflow is set up locally using Docker and accessed via the web interface at `http://localhost:8080`. After setting up the Airflow environment with Docker Compose and ensuring all DAG's and scripts are correctly mounted inside the containers, the Airflow web UI can be used to trigger the DAG manually. The DAG automatically handles the transformation and data posting steps as part of its execution flow.

Meanwhile, the HAPI FHIR server is run separately in a Google Cloud Shell environment using Docker, by pulling the official HAPI image and exposing it on port `8080`. This allows the Airflow DAG (running locally) to interact with the FHIR server hosted in the cloud by posting data via the server's REST API endpoint. After successful execution, the uploaded FHIR resources (e.g., `Patient`) can be verified by visiting the server's browser-based Swagger interface or by querying the appropriate FHIR endpoints.




