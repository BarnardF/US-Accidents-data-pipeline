# US Accidents Data Analysis Pipeline



## Project Overview

This project implements a comprehensive data pipeline to process and analyze the US Accidents (2016-2023) dataset from Kaggle. Using batch processing, raw accident data is transformed into actionable insights presented via interactive dashboards. These dashboards highlight accident hotspots, temporal patterns, and contributing factors that can drive improvements in road safety and traffic management. A lot of the project was made using ai, as a test to see how far i can get, a lot of times it was made clear to me that will not suffice and that i need to improve my own skills and knowlegde before using ai like in this project again.

### Key Accomplishments

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

The project leverages the following technologies:

- **Docker + Apache Airflow**: For workflow orchestration and batch processing management
- **Google Cloud Storage (GCS)**: Raw and processed data storage with structured bucket organization
- **Google BigQuery**: Heavy-duty data processing and analytics with optimized partitioning and clustering
- **Dataform**: Managing SQL transformation logic for accident data analysis
- **Looker Studio**: Interactive visualization dashboards


## Pipeline Workflow

![dataflow chart](https://github.com/user-attachments/assets/6324862f-8da3-4dd2-960e-ca174de3cd1f)

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

## Dataform Transformation Layer

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

## Dashboard Development

![Screenshot (1399)](https://github.com/user-attachments/assets/9555b929-a91f-4a05-8049-94d27eee7ce5)


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

- **April 9-10, 2025**: Initial Setup Phase
- **April 11-13, 2025**: Data Processing Phase
- **April 14-15, 2025**: GCP Integration Phase
- **April 16, 2025**: BigQuery Integration Phase
- **April 17-25, 2025**: Transformation Development Phase (Current)
- **April 26-May 9, 2025**: Analytics Layer & Dashboard Development Phase
- **May 10-16, 2025**: Testing & Documentation Phase


## Acknowledgment

This README was created with the assistance of AI (ChatGPT by OpenAI) to ensure clarity and consistency.

---

*Last Updated: May 3, 2025*
