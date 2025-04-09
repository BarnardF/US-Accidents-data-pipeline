# US Accidents Data Pipeline

This project builds a data pipeline to process and analyze the US Accidents (2016-2023) dataset from Kaggle.

## Project Structure
- `dags/`: Contains Apache Airflow DAG definitions
- `data/`: Directory for storing data files
  - `raw/`: Raw data files downloaded from Kaggle
  - `processed/`: Processed data files
- `logs/`: Airflow logs
- `plugins/`: Custom Airflow plugins

## Setup Instructions
1. Install Docker and Docker Compose
2. Clone this repository
3. Navigate to the project directory
4. Run `docker-compose up -d`
5. Access Airflow at http://localhost:8080 (username: airflow, password: airflow)
6. Update your Kaggle credentials in the DAG file before running it

## Dataset
US Accidents (2016 - 2023) from Kaggle:
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
