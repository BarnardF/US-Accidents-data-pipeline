# US-Accidents-Data-Engineering-project
This is my project submission for data engineering zoomcamp 2025


# Data Pipeline Project Summary

## **Project Overview:**
The goal of this project is to build a robust data pipeline for processing and visualizing data. The pipeline will collect, transform, store, and visualize data using a combination of **GitHub Codespaces**, **Docker**, **Google Cloud Platform (GCP)**, and other modern tools. The project will culminate in the creation of interactive dashboards to present meaningful insights.

---

## **Tech Stack:**

1. **GitHub Codespaces + Docker for Apache Airflow**  
   - **Purpose**: This environment will serve as the local development environment for building and orchestrating the data pipeline.
   - **Why**: GitHub Codespaces allows for easy setup and sharing of your codebase, while Docker provides a consistent, isolated environment for running Apache Airflow and its dependencies.

2. **GCP Bucket for Data Lake (Storage)**  
   - **Purpose**: Store raw datasets in Google Cloud Storage (GCS) as a Data Lake before further processing.
   - **Why**: GCS is scalable, secure, and integrates well with BigQuery for future data storage and analysis.

3. **Data Transformation & Storage in BigQuery**  
   - **dbt (Data Build Tool)**  
     - **Purpose**: Perform SQL-based transformations directly in BigQuery, organizing the raw data into a more usable format for analysis.
     - **Why**: dbt simplifies managing transformations, testing, and documentation of data pipelines.

4. **Looker Studio (Google Data Studio) for Dashboard**  
   - **Purpose**: Use Looker Studio (formerly Google Data Studio) to create interactive dashboards and visualizations for data insights.
   - **Why**: Looker Studio integrates seamlessly with BigQuery, allowing easy creation of dynamic reports and dashboards.

---

## **What Has Been Done So Far:**
1. **GitHub Codespaces Environment Created**  
   - You've set up a Codespace, which is now the development environment for your project.
   - The environment will house the Apache Airflow setup (inside a Docker container).

2. **Dockerized Apache Airflow**  
   - Plan: You'll run Apache Airflow in Docker to manage the orchestration of your data pipeline (from raw data ingestion to final storage in BigQuery).
   - Docker Compose will be used to manage the various components required to run Airflow (e.g., Webserver, Scheduler, Database).

---

## **Next Steps & What Needs to Be Done:**

1. **Set Up Apache Airflow in Docker**  
   - **Action**: Configure and run Apache Airflow using Docker. Create the necessary Dockerfiles and Docker Compose files to define the Airflow components.
   - **Goal**: Ensure that Airflow is running and able to orchestrate tasks effectively.

2. **Create Airflow DAGs**  
   - **Action**: Develop Airflow Directed Acyclic Graphs (DAGs) that will orchestrate the process flow of your pipeline. DAGs will handle tasks like data ingestion, transformation, and storage.
   - **Goal**: Ensure tasks are correctly defined with proper dependencies and that Airflow can handle task scheduling and execution.

3. **Set Up GCP Bucket for Data Lake**  
   - **Action**: Create a GCP bucket for storing raw data. Integrate it into your Airflow DAGs to store incoming datasets.
   - **Goal**: Ensure data can be ingested into the GCS bucket, serving as your data lake.

4. **Data Transformation with dbt**  
   - **Action**: Set up dbt to run transformations in BigQuery. You'll need to write dbt models to clean and transform the raw data stored in BigQuery.
   - **Goal**: Define a clean and structured dataset in BigQuery, making it ready for analysis and visualization.

5. **Load Data into BigQuery**  
   - **Action**: Use dbt to load the transformed data into BigQuery for analysis.
   - **Goal**: Ensure the data is accessible in BigQuery, where it can be queried for reporting and analysis.

6. **Set Up Looker Studio for Data Visualization**  
   - **Action**: Connect BigQuery to Looker Studio to visualize the data in dashboards.
   - **Goal**: Create interactive dashboards that provide valuable insights and can be filtered based on user input.

7. **Testing & Optimization**  
   - **Action**: Test your pipeline for functionality and optimize for performance. Ensure your Airflow DAGs are running smoothly and your data transformations are efficient.
   - **Goal**: Ensure that all tasks run without errors and the pipeline is scalable.

8. **Documentation & Finalizing Project**  
   - **Action**: Document the entire pipeline and its components, including setup instructions for the Docker container, details of Airflow DAGs, dbt models, and Looker Studio dashboards.
   - **Goal**: Ensure reproducibility, clarity, and ease of understanding for anyone reviewing your project.

---

## **Summary of What’s Been Decided:**
- **Tech Stack**:
  - **GitHub Codespaces + Docker for Apache Airflow**: Orchestrate and manage the data pipeline.
  - **GCP Bucket for Data Lake**: Store raw data.
  - **dbt**: Transform data stored in BigQuery.
  - **Looker Studio**: Visualize and create dashboards based on the data stored in BigQuery.

- **What’s Been Done**:
  - GitHub Codespaces environment set up.
  - Planning and structure for using Docker to run Apache Airflow.
  
- **Next Steps**:
  - Set up and configure Apache Airflow with Docker.
  - Develop and test Airflow DAGs for orchestration.
  - Set up GCP Bucket for data storage.
  - Transform data with dbt and store it in BigQuery.
  - Build visualizations in Looker Studio.
  - Test and optimize the pipeline.
  - Document the project.

