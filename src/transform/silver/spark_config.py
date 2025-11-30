"""
Optimized Spark configuration for Silver SCD2 processing
Place these configurations in your spark-defaults.conf or set them programmatically
"""

# Performance-optimized Spark configurations for Silver SCD2 processing

SPARK_PERFORMANCE_CONFIG = {
    # === Adaptive Query Execution (AQE) ===
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "200",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    
    # === Broadcasting and Joins ===
    "spark.sql.autoBroadcastJoinThreshold": "50MB",
    "spark.sql.broadcastTimeout": "300s",
    
    # === Cost-Based Optimizer ===
    "spark.sql.cbo.enabled": "true",
    "spark.sql.cbo.joinReorder.enabled": "true",
    "spark.sql.cbo.planStats.enabled": "true",
    "spark.sql.cbo.starSchemaDetection": "true",
    
    # === Memory Management ===
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "128MB",
    
    # === Serialization ===
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.unsafe": "true",
    "spark.kryoserializer.buffer.max": "1024m",
    
    # === Delta Lake Optimizations ===
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.autoCompact.minNumFiles": "50",
    "spark.databricks.delta.merge.enableLowShuffle": "true",
    "spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled": "true",
    
    # === Streaming Optimizations ===
    "spark.sql.streaming.metricsEnabled": "true",
    "spark.sql.streaming.ui.enabled": "true",
    "spark.sql.streaming.checkpointLocation.compressed": "true",
    
    # === File I/O Optimizations ===
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
    "spark.sql.files.openCostInBytes": "4194304",      # 4MB
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.enableVectorizedReader": "true",
    
    # === Caching ===
    "spark.sql.inMemoryColumnarStorage.compressed": "true",
    "spark.sql.inMemoryColumnarStorage.batchSize": "20000",
    
    # === Dynamic Allocation (if supported) ===
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "20",
    "spark.dynamicAllocation.initialExecutors": "4",
    "spark.dynamicAllocation.executorIdleTimeout": "300s",
    
    # === Speculation (for handling slow tasks) ===
    "spark.speculation": "true",
    "spark.speculation.interval": "100ms",
    "spark.speculation.multiplier": "1.5",
}

# Memory-specific configurations (adjust based on your cluster)
MEMORY_CONFIG_SMALL = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.executor.memoryFraction": "0.8",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "5000",
}

MEMORY_CONFIG_MEDIUM = {
    "spark.executor.memory": "4g", 
    "spark.driver.memory": "2g",
    "spark.executor.memoryFraction": "0.8",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
}

MEMORY_CONFIG_LARGE = {
    "spark.executor.memory": "8g",
    "spark.driver.memory": "4g", 
    "spark.executor.memoryFraction": "0.8",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "20000",
}

def apply_performance_config(spark_session, memory_profile="medium"):
    """
    Apply performance configurations to an existing Spark session
    
    Args:
        spark_session: Active Spark session
        memory_profile: "small", "medium", or "large"
    """
    
    # Apply core performance configurations
    for key, value in SPARK_PERFORMANCE_CONFIG.items():
        try:
            spark_session.conf.set(key, value)
        except Exception as e:
            print(f"Warning: Could not set {key}: {e}")
    
    # Apply memory-specific configurations
    memory_configs = {
        "small": MEMORY_CONFIG_SMALL,
        "medium": MEMORY_CONFIG_MEDIUM, 
        "large": MEMORY_CONFIG_LARGE
    }
    
    if memory_profile in memory_configs:
        for key, value in memory_configs[memory_profile].items():
            try:
                spark_session.conf.set(key, value)
            except Exception as e:
                print(f"Warning: Could not set memory config {key}: {e}")
    
    print(f"Applied performance configuration with {memory_profile} memory profile")

def print_current_config(spark_session):
    """Print current Spark configuration for debugging"""
    print("=== Current Spark Configuration ===")
    
    important_configs = [
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled", 
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.databricks.delta.optimizeWrite.enabled",
        "spark.executor.memory",
        "spark.driver.memory"
    ]
    
    for config in important_configs:
        try:
            value = spark_session.conf.get(config)
            print(f"{config}: {value}")
        except:
            print(f"{config}: Not set")
    
    print("=" * 40)

# Recommended configuration for docker-compose spark-defaults.conf
SPARK_DEFAULTS_CONF = """
# Performance Optimizations for SCD2 Processing
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.coalescePartitions.minPartitionSize 64MB
spark.sql.adaptive.advisoryPartitionSizeInBytes 128MB
spark.sql.adaptive.skewJoin.enabled true
spark.sql.adaptive.localShuffleReader.enabled true

spark.sql.autoBroadcastJoinThreshold 50MB
spark.sql.cbo.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer

# Delta Lake Optimizations  
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true
spark.databricks.delta.merge.enableLowShuffle true

# Memory and I/O
spark.sql.files.maxPartitionBytes 134217728
spark.sql.parquet.compression.codec snappy
spark.sql.inMemoryColumnarStorage.compressed true

# Streaming
spark.sql.streaming.metricsEnabled true
spark.sql.streaming.checkpointLocation.compressed true
"""

if __name__ == "__main__":
    print("Spark Performance Configuration")
    print("=" * 40)
    print("To use these configurations:")
    print("1. Add to spark-defaults.conf in your Spark installation")
    print("2. Or call apply_performance_config(spark) in your code")
    print("3. Or set individual configurations programmatically")
    print("\nRecommended spark-defaults.conf content:")
    print(SPARK_DEFAULTS_CONF)