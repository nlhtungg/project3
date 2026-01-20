from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,  # Retry 3 times on failure
    'retry_delay': timedelta(seconds=60),  # Wait 60 seconds before retry
}

# Define the DAG
with DAG(
    dag_id="cdragon_data_pipeline",
    description="Crawl TFT data from Community Dragon and parse to CSV files",
    default_args=default_args,
    start_date=datetime(2025, 10, 29),
    schedule_interval=timedelta(hours=24),
    catchup=False,
    tags=["cdragon", "crawl", "parse", "tft"]
) as dag:

    # Task 1: Crawl data from Community Dragon
    crawl_task = BashOperator(
        task_id="Crawl_Game_Data_from_CDragon",
        bash_command="python /opt/src/ingest/crawl_cdragon.py",
        retries=3,
        retry_delay=timedelta(seconds=60)
    )

    # Task 2: Parse the crawled data to CSV
    parse_task = BashOperator(
        task_id="Parse_Crawled_Data_to_CSV",
        bash_command="python /opt/src/ingest/parse_cdragon.py",
        retries=3,
        retry_delay=timedelta(seconds=60)
    )

    insert_task = SparkSubmitOperator(
        task_id="Insert_Parsed_Data_to_Bronze",
        application="/opt/src/transform/bronze/batchTables.py",
        conn_id="spark_master",
        verbose=True,
        name="InsertCDragonDataToDelta",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf",
        retries=0
    )

    transform_task = SparkSubmitOperator(
        task_id="Insert_Parsed_Data_to_Silver",
        application="/opt/src/transform/silver/batchTables.py",
        conn_id="spark_master",
        verbose=True,
        name="InsertCDragonDataToDelta",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf",
        retries=0
    )

    # Set task dependencies
    crawl_task >> parse_task >> insert_task >> transform_task