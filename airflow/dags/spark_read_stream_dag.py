from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG định nghĩa cho Spark Read Streaming job
with DAG(
    dag_id="spark_read_stream_from_delta",
    description="Submit Spark Structured Streaming job to read from Delta Lake and process data",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 10, 29),
    catchup=False,
    tags=["spark", "streaming", "delta_lake", "read", "test"]
) as dag:

    spark_read_stream_task = SparkSubmitOperator(
        task_id="submit_read_stream_job",
        application="/opt/spark-apps/test_read_stream.py",
        conn_id="spark_master",
        verbose=True,
        name="SparkReadStreamDeltaTest",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf",
        # Optional: Add executor memory configs if needed
        # conf={
        #     "spark.executor.memory": "1g",
        #     "spark.driver.memory": "1g"
        # }
    )

    spark_read_stream_task
