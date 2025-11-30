# src/common/io/spark_session.py
from pyspark.sql import SparkSession
from common.config.delta import get_delta_config
from typing import Literal, Dict, Optional
import os

JobType = Literal["bronze", "silver", "gold", "batch"]

# Resource allocation profiles for different job types
RESOURCE_PROFILES = {
    "bronze": {
        # Bronze jobs are I/O intensive (Kafka -> Delta)
        "executor_memory": "1g",
        "driver_memory": "512m",
        "executor_cores": "1",
        "max_executors": "3",
        "min_executors": "1",
        "initial_executors": "1",
        "sql_shuffle_partitions": "50",
        "streaming_backpressure": "true",
        "streaming_receiver_max_rate": "1000"
    },
    "silver": {
        # Silver jobs are CPU intensive (SCD2 processing)
        "executor_memory": "2g",
        "driver_memory": "1g", 
        "executor_cores": "2",
        "max_executors": "4",
        "min_executors": "1",
        "initial_executors": "2",
        "sql_shuffle_partitions": "100",
        "streaming_backpressure": "true",
        "streaming_receiver_max_rate": "500"
    },
    "gold": {
        # Gold jobs are analytical (aggregations)
        "executor_memory": "2g",
        "driver_memory": "1g",
        "executor_cores": "2", 
        "max_executors": "5",
        "min_executors": "1",
        "initial_executors": "2",
        "sql_shuffle_partitions": "200",
        "streaming_backpressure": "true",
        "streaming_receiver_max_rate": "200"
    },
    "batch": {
        # Batch jobs can use more resources when available
        "executor_memory": "3g",
        "driver_memory": "2g",
        "executor_cores": "2",
        "max_executors": "6",
        "min_executors": "2",
        "initial_executors": "3",
        "sql_shuffle_partitions": "200",
        "streaming_backpressure": "false",
        "streaming_receiver_max_rate": "10000"
    }
}

def create_spark(app_name: str = "LakehouseApp", job_type: JobType = "batch") -> SparkSession:
    """
    Create resource-aware SparkSession with Delta + Hive + MinIO/S3A.
    Supports dynamic resource allocation based on job type.
    
    Args:
        app_name: Application name
        job_type: Type of job (bronze/silver/gold/batch) for resource allocation
    """
    cfg = get_delta_config()
    profile = RESOURCE_PROFILES[job_type]
    
    # Create unique app name with job type to avoid conflicts
    full_app_name = f"{app_name}-{job_type}"
    
    builder = (
        SparkSession.builder
        .appName(full_app_name)
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
        .config("spark.eventLog.enabled", "false")  # Disable event logging to avoid S3 directory issues
        .enableHiveSupport()
    )

    # Apply resource profile
    builder = apply_resource_profile(builder, profile, job_type)
    
    # Hive metastore (nếu có)
    if cfg.hive_metastore_uri:
        builder = builder.config("hive.metastore.uris", cfg.hive_metastore_uri)

    # MinIO / S3A configuration
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
    
    print(f"Created Spark session '{full_app_name}' with {job_type} resource profile")
    print_resource_allocation(profile)
    
    return spark

def apply_resource_profile(builder, profile: Dict, job_type: JobType):
    """
    Apply resource configuration based on job type profile
    """
    
    # Dynamic allocation settings
    builder = (
        builder
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", profile["min_executors"])
        .config("spark.dynamicAllocation.maxExecutors", profile["max_executors"])
        .config("spark.dynamicAllocation.initialExecutors", profile["initial_executors"])
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "120s")
        .config("spark.dynamicAllocation.schedulerBacklogTimeout", "10s")
    )
    
    # Memory and core allocation
    builder = (
        builder
        .config("spark.executor.memory", profile["executor_memory"])
        .config("spark.driver.memory", profile["driver_memory"])
        .config("spark.executor.cores", profile["executor_cores"])
        .config("spark.sql.shuffle.partitions", profile["sql_shuffle_partitions"])
    )
    
    # Streaming-specific configurations
    if job_type in ["bronze", "silver"]:
        builder = (
            builder
            .config("spark.streaming.backpressure.enabled", profile["streaming_backpressure"])
            .config("spark.streaming.receiver.maxRate", profile["streaming_receiver_max_rate"])
            .config("spark.streaming.kafka.maxRatePerPartition", profile["streaming_receiver_max_rate"])
            # Optimize for streaming workloads
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.sql.streaming.ui.enabled", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
        )
    
    # Job-specific optimizations
    if job_type == "bronze":
        # Optimize for high-throughput ingestion
        builder = (
            builder
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32MB")
            .config("spark.serializer.objectStreamReset", "10000")
        )
    elif job_type == "silver":
        # Optimize for complex transformations
        builder = (
            builder
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            # Enable broadcast join for SCD2 lookups
            .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        )
    
    return builder

def print_resource_allocation(profile: Dict):
    """
    Print current resource allocation for debugging
    """
    print(f"Resource Allocation:")
    print(f"  Executor Memory: {profile['executor_memory']}")
    print(f"  Driver Memory: {profile['driver_memory']}")
    print(f"  Executor Cores: {profile['executor_cores']}")
    print(f"  Executors: {profile['min_executors']}-{profile['max_executors']} (initial: {profile['initial_executors']})")
    print(f"  Shuffle Partitions: {profile['sql_shuffle_partitions']}")

def create_bronze_spark(app_name: str = "Bronze-Stream") -> SparkSession:
    """Create Spark session optimized for bronze layer streaming"""
    return create_spark(app_name, "bronze")

def create_silver_spark(app_name: str = "Silver-SCD2") -> SparkSession:
    """Create Spark session optimized for silver layer SCD2 processing"""
    return create_spark(app_name, "silver")

def create_gold_spark(app_name: str = "Gold-Analytics") -> SparkSession:
    """Create Spark session optimized for gold layer analytics"""
    return create_spark(app_name, "gold")
