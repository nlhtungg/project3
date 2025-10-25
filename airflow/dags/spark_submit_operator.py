from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG định nghĩa
with DAG(
    dag_id="spark_etl_to_delta_lake",
    description="Submit Spark job ghi dữ liệu vào Delta Lake trên MinIO",
    catchup=False,
    tags=["spark", "delta_lake", "etl"]
) as dag:

    spark_submit_task = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/spark-apps/sample_spark_job.py",
        conn_id="spark_master",
        verbose=True,
        name="SparkDeltaLakeETL",
        deploy_mode="client",
        properties_file="/opt/spark-config/spark-defaults.conf"
    )

    spark_submit_task
