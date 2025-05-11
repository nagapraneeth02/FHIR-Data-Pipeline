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


