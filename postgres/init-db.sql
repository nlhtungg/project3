-- Create databases and users for both Airflow and Hive Metastore

-- Create Airflow database and user
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

-- Create Hive Metastore database and user
CREATE DATABASE hive;
CREATE USER hive WITH ENCRYPTED PASSWORD 'hivepw';
GRANT ALL PRIVILEGES ON DATABASE hive TO hive;
ALTER DATABASE hive OWNER TO hive;