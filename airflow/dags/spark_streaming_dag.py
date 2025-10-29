from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG định nghĩa cho Spark Streaming job
with DAG(
    dag_id="spark_streaming_to_delta_lake",
    description="Submit Spark Structured Streaming job to write data to Delta Lake",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 10, 29),
    catchup=False,
    tags=["spark", "streaming", "delta_lake", "test"]
) as dag:

    spark_streaming_task = SparkSubmitOperator(
        task_id="submit_streaming_job",
        application="/opt/spark-apps/test_stream.py",
        conn_id="spark_master",
        verbose=True,
        name="SparkStreamingDeltaTest",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf",
        # Optional: Add executor memory configs if needed
        # conf={
        #     "spark.executor.memory": "1g",
        #     "spark.driver.memory": "1g"
        # }
    )

    spark_streaming_task
