from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG định nghĩa
with DAG(
    dag_id="spark_bash",
    description="Submit Spark job ghi dữ liệu vào Delta Lake trên MinIO",
    catchup=False,
    tags=["spark", "delta_lake", "etl"]
) as dag:

    spark_submit_task = BashOperator(
        task_id="submit_spark_job",
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --name SparkDeltaLake \
            /opt/airflow/jobs/sample_spark_job.py
        """
    )

    spark_submit_task
