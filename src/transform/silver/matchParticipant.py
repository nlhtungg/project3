from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
from delta.tables import DeltaTable
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_silver_spark
from common.io.delta_io import build_paths, ensure_database, register_delta_table, read_stream_from_delta
from common.config.delta import get_delta_config
from transform.silver.spark_config import apply_performance_config


def process_match_participant_batch(spark, bronze_df, silver_data_path, batch_id):
    """Process match participant data - append with Delta merge schema"""
    batch_timestamp = F.current_timestamp()
    
    # Deduplicate within the batch first (much cheaper than table-wide merge)
    deduplicated_df = (
        bronze_df
        .dropDuplicates(["match_id", "gameId", "puuid"])
        .withColumn("processed_ts", batch_timestamp)
        .withColumn("year", F.year(batch_timestamp))
        .withColumn("month", F.month(batch_timestamp))
        .withColumn("day", F.dayofmonth(batch_timestamp))
        .select(
            "match_id", "gameId", "puuid", "placement", "gold_left", "last_round",
            "level", "players_eliminated", "total_damage_to_players",
            "processed_ts", "year", "month", "day"
        )
    )
    
    # Check if table exists and has correct schema
    try:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, silver_data_path)
        existing_schema = delta_table.toDF().schema
        
        # If table exists but has wrong partitioning or empty schema, recreate it
        if len(existing_schema.fields) == 0:
            print("Table exists with empty schema - recreating with correct schema")
            (deduplicated_df
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("year", "month", "day")
             .save(silver_data_path))
        else:
            # Normal append
            (deduplicated_df
             .write
             .format("delta")
             .mode("append")
             .partitionBy("year", "month", "day")
             .save(silver_data_path))
    except Exception as e:
        # Table doesn't exist, create it
        print(f"Creating new table: {e}")
        (deduplicated_df
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("year", "month", "day")
         .save(silver_data_path))
    
    print(f"Processed {deduplicated_df.count()} match participant records")


def create_match_participant_silver_stream(spark: SparkSession, cfg=None, max_bytes_per_trigger="50m"):
    """Create and return streaming query for TFT match participant silver layer with append-only writes
    
    Args:
        spark: SparkSession to use
        cfg: Delta configuration
        max_bytes_per_trigger: Maximum data size to process per trigger (e.g., "50m", "100m")
    """
    if cfg is None:
        cfg = get_delta_config()
    
    apply_performance_config(spark, memory_profile="medium")
    
    # Bronze layer paths
    bronze_layer = "bronze"
    bronze_table_name = "tft_match_participants"
    bronze_paths = build_paths(bronze_layer, bronze_table_name, cfg)
    
    # Silver layer paths
    silver_layer = "silver"
    silver_table_name = "tft_match_participants"
    silver_paths = build_paths(silver_layer, silver_table_name, cfg)
    
    ensure_database(spark, silver_paths.db_name)
    
    # Read from bronze layer as stream with micro batch size limit
    bronze_stream = read_stream_from_delta(
        spark,
        bronze_paths.data_path,
        max_bytes_per_trigger=max_bytes_per_trigger
    )
    
    # Select only required columns
    selected_df = (
        bronze_stream
        .select(
            "match_id", "gameId", "puuid", "placement", "gold_left",
            "last_round", "level", "players_eliminated", "total_damage_to_players"
        )
        .filter(F.col("gameId").isNotNull())
        .filter(F.col("puuid").isNotNull())
    )
    
    # Process each micro-batch with append-only writes
    def process_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            print(f"Processing match participant batch {batch_id}")
            process_match_participant_batch(spark, batch_df, silver_paths.data_path, batch_id)
            print(f"Batch {batch_id} completed successfully")
        else:
            print(f"Batch {batch_id}: No records to process")
    
    # Start the streaming query
    query = (
        selected_df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", silver_paths.checkpoint_path)
        .trigger(processingTime="60 seconds")
        .queryName("silver-match-participant")
        .start()
    )
    
    # Register the silver table
    register_delta_table(
        spark,
        silver_paths.db_name,
        silver_paths.table_name,
        silver_paths.data_path
    )
    
    print(f"Silver table registered: {silver_paths.full_table_name}")
    print(f"Data path: {silver_paths.data_path}")
    print(f"Checkpoint path: {silver_paths.checkpoint_path}")
    
    return query


def main():
    spark = create_silver_spark("silver-match-participant")
    
    try:
        print("Starting streaming...")
        query = create_match_participant_silver_stream(spark)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        if 'query' in locals():
            query.stop()


if __name__ == "__main__":
    main()
