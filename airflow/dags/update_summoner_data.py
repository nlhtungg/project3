from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 10,                 # Streaming nên cho phép retry nhiều lần nếu crash
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="update_summoner_data_streaming",
    description="Run Parallel Spark Streaming Jobs for Summoner Data",
    default_args=default_args,
    start_date=datetime(2023, 10, 29), 
    schedule_interval='@once', 
    catchup=False,
    tags=["summoner", "streaming", "tft"]
) as dag:
    
    # --- TASK 1: CRAWLER ---
    crawl_task = BashOperator(
        task_id="Get_Summoner_Data_Stream_Producer",
        bash_command="python /opt/src/ingest/summoner.py",
        retries=3
    )

    # --- TASK 2: STREAMING BRONZE ---
    insert_task = SparkSubmitOperator(
        task_id="Insert_Summoner_Data_to_Bronze_Stream",
        application="/opt/src/transform/bronze/summoner.py",
        conn_id="spark_master",
        verbose=True,
        name="InsertSummonerDataToDelta_Stream",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf"
    )

    # --- TASK 3: STREAMING SILVER (SCD) ---
    scd_task = SparkSubmitOperator(
        task_id="Insert_Summoner_Data_to_Silver_Stream",
        application="/opt/src/transform/silver/summoner.py",
        conn_id="spark_master",
        verbose=True,
        name="ApplySCD2ToSummonerData_Stream",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf"
    )