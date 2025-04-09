# US Accidents Data Pipeline Project

## Project Overview
This project will build a data pipeline to process and analyze the US Accidents (2016-2023) dataset from Kaggle. The pipeline will use batch processing to transform raw accident data into actionable insights that can help improve road safety and traffic management. The final output will be interactive dashboards highlighting accident hotspots, temporal patterns, and contributing factors.

## Business Problem to Solve
This pipeline will help address the following real-world problems:

1. **Traffic Safety Improvement**: Identify high-risk accident zones and contributing factors to help authorities implement targeted safety measures.

2. **Emergency Response Optimization**: Analyze temporal and seasonal patterns to optimize the deployment of emergency response resources.

3. **Urban Planning Support**: Provide insights into how road design, traffic control systems, and weather conditions correlate with accident frequency and severity.

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

## Tech Stack

### Docker + Apache Airflow (Local Setup)
- **Purpose**: Orchestrate the batch processing workflow for accident data.
- **Setup**: Simple Docker container setup without Astronomer CLI.
- **Processing**: Schedule weekly/monthly batch processing of accident data.

### GCP Bucket for Data Storage
- **Purpose**: Store the raw US Accidents dataset and processed intermediate files.
- **Implementation**: Create a structured folder system for raw, processed, and archived data.

### Google BigQuery for Data Processing
- **Purpose**: Perform heavy analytical queries and transformations on the accident data.
- **Implementation**: Create tables for raw data, intermediate processing, and final analytical views.

### Dataform for Transformations
- **Purpose**: Manage SQL transformation logic for accident data analysis.
- **Implementation**: Create repeatable transformation workflows with version control.

### Looker Studio for Dashboards
- **Purpose**: Create interactive visualizations of accident analytics.
- **Implementation**: Build dashboards focusing on geographical hotspots, time patterns, and contributing factors.

## Implementation Plan

### 1. Data Understanding & Initial Setup (Week 1)
- Download and explore the US Accidents dataset from Kaggle
- Set up Docker environment with Apache Airflow
- Create GCS bucket structure and BigQuery datasets
- Define schema and data quality checks

### 2. Data Ingestion Pipeline (Week 1-2)
- Create Airflow DAG to load the Kaggle dataset into GCS
- Implement initial data validation checks
- Set up batch processing schedule (e.g., weekly updates)
- Load raw data into BigQuery staging tables

### 3. Data Transformation (Week 2-3)
- Set up Dataform project to transform accident data
- Create transformations for:
  - Geographical aggregations (accident hotspots by state, city, and road)
  - Temporal analysis (time of day, day of week, seasonal patterns)
  - Weather impact analysis
  - Severity factor analysis
  - Year-over-year trend analysis

### 4. Analytics Layer (Week 3)
- Create final analytical views in BigQuery
- Set up aggregated tables for dashboard performance
- Implement data refresh processes

### 5. Dashboard Development (Week 3-4)
- Connect transformed data to Looker Studio
- Create interactive maps of accident hotspots
- Build visualizations for time-based patterns
- Develop filtering capabilities for exploring different factors

### 6. Testing, Documentation & Optimization (Week 4)
- End-to-end testing of the pipeline
- Document setup procedures and maintenance guidelines
- Optimize BigQuery queries and Dataform transformations
- Finalize dashboards and user guides

## Specific Analysis Opportunities

### Geographical Analysis
- Accident hotspot mapping at different geographical levels
- Road segment risk scoring
- Intersection danger analysis
- Urban vs. rural accident pattern comparison

### Temporal Analysis
- Rush hour accident patterns
- Weekend vs. weekday comparison
- Holiday period risk assessment
- Year-over-year trend analysis

### Contributing Factor Analysis
- Weather impact on accident rates and severity
- Construction zone accident patterns
- Correlation between traffic volume and accidents
- Impact of road features (junctions, crossing types) on accidents

### Resource Allocation Insights
- Optimal emergency response unit positioning
- High-priority areas for traffic safety improvements
- Peak times requiring increased patrol presence

## Dashboard Examples

1. **Safety Administrator Dashboard**
   - Interactive map of accident hotspots
   - Severity distribution charts
   - Contributing factor analysis
   - Year-over-year trend comparison

2. **Emergency Response Planning Dashboard**
   - Hourly/daily accident frequency heat maps
   - Weather impact visualizations
   - Response time optimization recommendations
   - Seasonal pattern analysis

3. **Urban Planning Dashboard**
   - Road feature correlation with accidents
   - Traffic control effectiveness metrics
   - Construction zone impact analysis
   - Before/after analysis of safety improvements

## Technical Implementation Details

### Airflow DAG Structure
- Data extraction and validation DAG (triggered on dataset update)
- Weekly processing DAG for refreshing analytical views
- Monthly reporting DAG for generating comprehensive reports

### BigQuery Table Design
- Raw accident data table
- Geocoded and enriched accident table
- Aggregation tables (by location, time period, contributing factors)
- Analytical views for dashboard consumption

### Dataform Implementation
- Core transformation SQL scripts
- Data quality verification queries
- Documentation of business logic
- Testing assertions for data validation

