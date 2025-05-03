# US Accidents Data Analysis Pipeline

![dataflow chart](https://github.com/user-attachments/assets/6324862f-8da3-4dd2-960e-ca174de3cd1f)


## Project Overview

This project implements a comprehensive data pipeline to process and analyze the US Accidents (2016-2023) dataset from Kaggle. Through batch processing techniques, raw accident data is transformed into actionable insights presented via interactive dashboards. These dashboards highlight accident hotspots, temporal patterns, and contributing factors that can drive improvements in road safety and traffic management.

### 🏆 Key Accomplishments

- Successfully implemented an end-to-end data pipeline from data acquisition to visualization
- Created a simulation system for processing historical monthly data
- Built BigQuery integration with optimized partitioning and clustering
- Established the foundation for advanced analytics and visualization

## Business Value Delivered

This pipeline addresses several real-world problems:

1. **Traffic Safety Improvement**: The analysis identifies high-risk accident zones and contributing factors, enabling authorities to implement targeted safety measures where they're needed most.

2. **Emergency Response Optimization**: Temporal and seasonal pattern analysis helps optimize the deployment of emergency response resources, potentially saving lives through faster response times.

3. **Urban Planning Support**: Insights into correlations between road design, traffic control systems, and weather conditions with accident frequency provide valuable input for urban planning decisions.

## Dataset

The project uses the comprehensive US Accidents (2016-2023) dataset from Kaggle, containing over 7 million records of accidents across the continental United States:
- [US Accidents Dataset on Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)

## Tech Stack

![Tech Stack Diagram](https://via.placeholder.com/800x400.png?text=Tech+Stack+Diagram)

The project leverages the following technologies:

- **Docker + Apache Airflow**: For workflow orchestration and batch processing management
- **Google Cloud Storage (GCS)**: Raw and processed data storage with structured bucket organization
- **Google BigQuery**: Heavy-duty data processing and analytics with optimized partitioning and clustering
- **Dataform**: Managing SQL transformation logic for accident data analysis
- **Looker Studio**: Interactive visualization dashboards

## Project Structure

```
us-accidents-pipeline/
├── dags/                          # Apache Airflow DAG definitions
│   ├── us_accidents_download.py   # DAG for downloading Kaggle dataset
│   ├── us_accidents_chunk_csv.py  # DAG for splitting dataset into monthly chunks
│   ├── simulate_monthly_uploads.py # DAG for simulating monthly batch processing
│   └── us_accidents_load_to_bigquery.py # DAG for loading data to BigQuery
├── data/                          # Directory for data files
│   ├── raw/                       # Raw data downloaded from Kaggle
│   └── chunks/                    # Monthly data chunks
├── logs/                          # Airflow logs
├── plugins/                       # Custom Airflow plugins
├── dataform/                      # Dataform SQL transformation definitions
│   ├── sources/
│   ├── staging/
│   ├── intermediate/
│   └── analytics/
├── docker-compose.yml             # Docker configuration
├── setup-instructions.md          # DAG usage instructions
└── README.md                      # This file
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account
- Kaggle account (for dataset access)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/us-accidents-pipeline.git
   cd us-accidents-pipeline
   ```

2. Set up your environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your GCP credentials and Kaggle API key
   ```

3. Launch the environment:
   ```bash
   docker-compose up -d
   ```

4. Access Airflow at http://localhost:8080 (username: airflow, password: airflow)

5. Set up Airflow Variables:
   - Go to Admin → Variables
   - Add `gcs_bucket_name` with your GCS bucket name
   - Add Kaggle credentials if using the download DAG

6. Configure GCP Connection:
   - Go to Admin → Connections
   - Add a new connection with ID `google_cloud_default`
   - Set Connection Type to `Google Cloud`
   - Add your GCP project ID and credentials

7. Start the pipeline by triggering the DAGs in sequence:
   ```bash
   # If starting from scratch with raw data download
   docker-compose exec airflow airflow dags trigger us_accidents_download
   
   # To process monthly chunks
   docker-compose exec airflow airflow dags trigger us_accidents_chunk_csv
   
   # To simulate monthly uploads to GCS
   docker-compose exec airflow airflow dags trigger us_accidents_simulate_monthly_processing
   
   # To load data to BigQuery
   docker-compose exec airflow airflow dags trigger us_accidents_load_to_bigquery
   ```

## Pipeline Workflow

![Pipeline Workflow](https://via.placeholder.com/800x500.png?text=Data+Pipeline+Workflow)

The data pipeline follows these key steps:

1. **Data Acquisition**: Downloads the US Accidents dataset from Kaggle
2. **Data Processing**: Splits the large dataset into monthly chunks for easier processing
3. **GCS Upload**: Uploads monthly data files to Google Cloud Storage
4. **BigQuery Loading**: Loads data into partitioned and clustered BigQuery tables
5. **Data Transformation**: Uses Dataform to transform raw data into analytical views
6. **Data Visualization**: Creates interactive dashboards in Looker Studio

## Monthly Simulation System

A unique feature of this pipeline is the monthly simulation system, which:

- Simulates processing historical data one month at a time
- Uses Airflow's scheduling features to process data in sequence
- Allows for testing and validation of the entire pipeline with controlled data volumes
- Provides a framework for handling incremental data updates

## BigQuery Integration

The BigQuery implementation includes:

- Partitioning by month for efficient querying of time-based data
- Clustering by State and Severity for optimized geographical and severity-based analysis
- Data quality inspection steps to validate loaded data
- Foundation for advanced analytical views

## Dataform Transformation Layer (In Progress)

The transformation layer is being implemented with Dataform and includes:

- Source definitions connecting to BigQuery raw tables
- Staging views for initial data cleanup
- Intermediate tables for specific analysis domains
- Final analytical views for dashboard consumption

### Key Transformations

1. **Geographical Analysis**:
   - Accident hotspot identification
   - State and city-level aggregations
   - Risk categorization

2. **Temporal Analysis**:
   - Time of day, day of week patterns
   - Monthly and seasonal trends
   - Year-over-year comparisons

3. **Weather Impact Analysis**:
   - Correlation between weather conditions and accidents
   - Temperature, visibility, and precipitation impact analysis
   - Seasonal weather pattern analysis

## Dashboard Development (Planned)

![Dashboard Example](https://via.placeholder.com/800x500.png?text=Accident+Hotspot+Dashboard)

Three main dashboards are being developed:

1. **Safety Administrator Dashboard**:
   - Interactive maps of accident hotspots with filtering capabilities
   - Severity distribution across different geographical areas
   - Contributing factor correlation analysis
   - Year-over-year trend comparison tools

2. **Emergency Response Planning Dashboard**:
   - Hourly and daily accident frequency heat maps
   - Weather impact visualizations
   - Response time optimization recommendations
   - Seasonal pattern analysis for resource planning

3. **Urban Planning Dashboard**:
   - Road feature correlation with accident frequency
   - Traffic control effectiveness metrics
   - Construction zone impact analysis
   - Before/after analysis of safety improvements

## Implementation Timeline

![Project Timeline](https://via.placeholder.com/800x200.png?text=Project+Timeline)

- **April 9-10, 2025**: Initial Setup Phase
- **April 11-13, 2025**: Data Processing Phase
- **April 14-15, 2025**: GCP Integration Phase
- **April 16, 2025**: BigQuery Integration Phase
- **April 17-25, 2025**: Transformation Development Phase (Current)
- **April 26-May 9, 2025**: Analytics Layer & Dashboard Development Phase
- **May 10-16, 2025**: Testing & Documentation Phase

## Next Steps

The immediate next steps for the project include:

1. **Complete Dataform Setup**:
   - Finalize transformation logic for geographical, temporal, and weather analysis
   - Create analytical views for dashboard consumption

2. **Develop Advanced Analytics**:
   - Implement year-over-year comparison views
   - Create road segment risk scoring
   - Develop intersection danger analysis

3. **Build Interactive Dashboards**:
   - Connect transformed data to Looker Studio
   - Create interactive maps and visualizations
   - Implement filtering capabilities

## Contact

For questions or further information about this project, please contact:

Your Name - [your.email@example.com](mailto:your.email@example.com)

---

*Last Updated: May 3, 2025*
