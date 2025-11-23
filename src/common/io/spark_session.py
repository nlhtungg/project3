# src/common/io/spark_session.py
from pyspark.sql import SparkSession
from common.config.delta import get_delta_config


def create_spark(app_name: str = "LakehouseApp") -> SparkSession:
    """
    Tạo SparkSession với Delta + Hive + MinIO/S3A.
    Dùng chung cho batch & stream, mọi layer.
    """
    cfg = get_delta_config()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
            "true",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.vacuum.logging.enabled", "true")
        .enableHiveSupport()
    )

    # Hive metastore (nếu có)
    if cfg.hive_metastore_uri:
        builder = builder.config("hive.metastore.uris", cfg.hive_metastore_uri)

    # MinIO / S3A (giống như bạn dùng trong summoner.py) :contentReference[oaicite:1]{index=1}
    builder = (
        builder.config("spark.hadoop.fs.s3a.access.key", cfg.s3a_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.s3a_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3a_endpoint)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", cfg.s3a_path_style_access)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", cfg.s3a_ssl_enabled)
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
