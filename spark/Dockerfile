FROM bitnamilegacy/spark:3.5.1

USER root

# Copy compatible Hadoop 3.3.4 libraries and other JARs
# This ensures S3A support matches Spark 3.5.1's bundled Hadoop version
COPY ./jars/*.jar /opt/bitnami/spark/jars/

USER 1001

# Install Delta Lake Python package
RUN pip install delta-spark==3.2.0 \
    && pip install python-dotenv