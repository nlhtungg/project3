FROM bitnamilegacy/spark:3.5.1

# Copy any additional JAR files into the Spark jars directory
COPY ./jars/*.jar /opt/bitnami/spark/jars/

# Config file
COPY ./spark/config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf