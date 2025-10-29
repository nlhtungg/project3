FROM apache/airflow:2.10.0-python3.11

USER root

# --- Install Java ---
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Install Spark 3.5.1 manually ---
RUN curl -L -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy compatible Hadoop 3.3.4 libraries and other JARs
# This ensures S3A support matches Spark 3.5.1's bundled Hadoop version
COPY ./jars/*.jar /opt/spark/jars/

# --- Switch back to airflow user ---
USER airflow

# pyspark version phải trùng Spark binary
RUN pip install pyspark==3.5.1 \
    && pip install pyarrow datetime requests beautifulsoup4 pendulum minio \
    && pip install apache-airflow-providers-apache-spark