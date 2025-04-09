FROM apache/airflow:2.7.1

USER root

# Install OS packages
RUN apt-get update && \
    apt-get install -y unzip curl

# Switch to airflow user before pip install
USER airflow

# Install Python packages
RUN pip install --no-cache-dir kaggle

