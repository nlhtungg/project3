FROM apache/airflow:2.7.3

USER root
# Cài đặt Docker CLI
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install pyarrow datetime requests beautifulsoup4 pendulum minio
RUN pip install apache-airflow-providers-apache-spark
RUN pip install apache-airflow-providers-docker
RUN pip install --upgrade apache-airflow-providers-openlineage