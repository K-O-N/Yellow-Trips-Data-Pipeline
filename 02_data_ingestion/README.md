# Data Ingestion with Airflow → Google Cloud Storage (GCS)

This document explains **how to start the Airflow ingestion pipeline from scratch** (Docker, credentials, configuration) and then describes **the ingestion tasks themselves**. It is intentionally concise and limited to what is required to run the DAGs successfully.

### Prerequisites and Setup
Docker and Docker Compose
Airflow is run **entirely in Docker**. No local Airflow installation is required.

##### Required:
* Docker
* Docker Compose
Airflow services (webserver, scheduler) are defined in `docker-compose.yml`.


### Google Cloud Project and GCS Bucket
Before starting Airflow, the following must exist in Google Cloud:
* A GCP project
* A GCS bucket to store raw data: The bucket is used as the landing zone for all ingested files.

### Google Cloud Credentials
Airflow authenticates to GCP using a **service account key**.
1. Create a service account in GCP
2. Grant permissions:
3. Download the service account key JSON
The credentials file is mounted into the Airflow container and referenced via an environment variable.


### Environment Variables: The following environment variables must be set in Docker:
* `GCP_PROJECT_ID`
* `GCP_GCS_BUCKET`
* `GOOGLE_APPLICATION_CREDENTIALS`
* `AIRFLOW_HOME` (defaults to `/opt/airflow/`)

These variables allow the DAGs to run without hardcoding project-specific values.


### Starting Airflow
From the project root:

```bash
docker-compose up -d
```

Verify the following containers are running:
* airflow-webserver: access the airflow ui on the port specificed 
* airflow-scheduler
