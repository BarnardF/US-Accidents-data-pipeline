# US Accidents Pipeline Project Documentation

## Project Overview
- **Project Name**: US Accidents Data Pipeline
- **Dataset**: [US Accidents (2016-2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) from Kaggle
- **Goal**: Build a data pipeline to process and analyze US traffic accident data, identify accident hotspots, temporal patterns, and contributing factors
- **Start Date**: April 9, 2025

## Project Log

### April 9, 2025 - Initial Setup

#### Completed Tasks:
1. **Project Planning and Structure:**
   - Defined project goals and problem statement
   - Selected US Accidents dataset from Kaggle as the data source
   - Outlined batch processing approach for data analysis
   - Identified key business problems to solve using this data

2. **Development Environment Setup:**
   - Created base project directory structure
   - Configured Docker Compose file for Apache Airflow
   - Set up necessary directories (dags, logs, plugins, data)
   - Created requirements.txt file with initial dependencies

3. **Initial DAG Creation:**
   - Created a DAG for downloading the US Accidents dataset from Kaggle
   - Implemented tasks for setting up Kaggle credentials and downloading data
   - Added verification steps to confirm successful data download

#### Technical Details:
- **Docker Configuration:**
  - Using Apache Airflow version 2.7.1
  - PostgreSQL 13 for Airflow metadata database
  - Redis for Celery executor task queue
  - CeleryExecutor for task distribution
  - Exposed Airflow webserver on port 8080

- **Project Structure:**
  ```
  us_accidents_pipeline/
  ├── docker-compose.yaml
  ├── requirements.txt
  ├── README.md
  ├── dags/
  │   └── us_accidents/
  │       ├── __init__.py
  │       └── download_kaggle_dataset.py
  ├── data/
  │   ├── raw/
  │   └── processed/
  ├── logs/
  └── plugins/
  ```

- **Initial DAG Components:**
  - Task 1: Setup Kaggle credentials
  - Task 2: Download US Accidents dataset
  - Task 3: Verify successful download

#### Next Steps:
1. **Environment Setup Completion:**
   - Install Docker and Docker Compose (if not already installed)
   - Run Docker Compose to start Airflow containers
   - Verify Airflow web UI is accessible at http://localhost:8080

2. **Dataset Acquisition:**
   - Create Kaggle account if needed
   - Generate Kaggle API key
   - Update DAG with personal Kaggle credentials
   - Execute the download_kaggle_dataset DAG

3. **GCP Setup for Next Phase:**
   - Create GCP account or project
   - Set up service account and credentials
   - Create GCS bucket for data storage
   - Configure BigQuery dataset

#### Notes and Observations:
- The US Accidents dataset is large (7-10GB) so ensure sufficient disk space
- Initial download may take considerable time depending on internet connection
- Consider implementing chunking strategies for initial data processing
- For local development, ensure Docker has adequate resources allocated (memory/CPU)

## Instructions for Next Session

1. **Start Containers:**
   ```bash
   cd us_accidents_pipeline
   docker-compose up -d
   ```

2. **Access Airflow UI:**
   - Open browser and navigate to http://localhost:8080
   - Login with username: airflow, password: airflow

3. **Update Kaggle Credentials:**
   - Edit dags/us_accidents/download_kaggle_dataset.py
   - Replace "YOUR_KAGGLE_USERNAME" and "YOUR_KAGGLE_KEY" with actual credentials

4. **Run the DAG:**
   - In Airflow UI, enable and trigger the "us_accidents_download" DAG
   - Monitor execution in the Airflow UI

5. **Verify Data Download:**
   - Check the data/raw directory for downloaded files
   - Confirm US_Accidents_Dec21_updated.csv or similar file exists
