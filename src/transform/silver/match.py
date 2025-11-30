from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DoubleType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import sys
from pathlib import Path
from typing import List, Tuple

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_silver_spark
from common.io.delta_io import build_paths, ensure_database, register_delta_table, read_stream_from_delta
from common.config.delta import get_delta_config
from transform.silver.spark_config import apply_performance_config

def create_scd2_merge_condition():
    """Create merge condition for SCD Type 2"""
    return """
    target.gameId = source.gameId
    AND target.match_id = source.match_id
    AND target.is_current = true
    """

def create_scd2_schema():
    """Define the schema for SCD Type 2 silver table"""
    return StructType([
        # Business keys
        StructField("match_id", StringType(), False),
        StructField("gameId", IntegerType(), False),
        
        # Attributes that change over time
        StructField("game_datetime", IntegerType(), True),
        StructField("game_length", DoubleType(), True),
        StructField("game_version", StringType(), True),
        StructField("tft_game_type", StringType(), True),
        StructField("tft_set_core_name", StringType(), True),
        StructField("tft_set_number", IntegerType(), True),
        
        # SCD Type 2 columns
        StructField("surrogate_key", StringType(), False),  # Surrogate key
        StructField("is_current", BooleanType(), False),    # Current record flag
        StructField("effective_date", TimestampType(), False),  # When record became effective
        StructField("end_date", TimestampType(), True),     # When record expired (null for current)
        StructField("hash_key", StringType(), False),       # Hash of changing attributes for comparison
        
        # Metadata
        StructField("processed_ts", TimestampType(), False), # Silver processing timestamp
        
        # Partitioning columns
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
    ])

def create_hash_key(df):
    """Create hash key from changing attributes for comparison - optimized version"""
    return F.sha2(
        F.concat_ws("||", 
            F.coalesce(F.col("game_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("game_length").cast("string"), F.lit("")),
            F.coalesce(F.col("game_version"), F.lit("")),
            F.coalesce(F.col("tft_game_type"), F.lit("")),
            F.coalesce(F.col("tft_set_core_name"), F.lit("")),
            F.coalesce(F.col("tft_set_number").cast("string"), F.lit(""))
        ), 256
    )

def process_scd2_batch(spark, bronze_df, silver_data_path):
    """Process SCD Type 2 logic for a batch of data - Performance Optimized"""
    
    if bronze_df.count() == 0:
        return
    
    # Cache the input dataframe to avoid recomputation
    bronze_df.cache()
    
    # Generate a single timestamp for the entire batch to ensure consistency
    batch_timestamp = F.current_timestamp()
    
    # Prepare the incoming data with optimized transformations
    processed_df = (
        bronze_df
        .withColumn("processed_ts", batch_timestamp)
        .withColumn("effective_date", batch_timestamp)
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("hash_key", create_hash_key(bronze_df))
        .withColumn("surrogate_key", 
                   F.concat(
                       F.col("match_id"),
                       F.lit("_"),
                       F.col("gameId").cast("string"), 
                       F.lit("_"),
                       F.date_format(batch_timestamp, "yyyyMMddHHmmssSSS"),
                       F.lit("_"),
                       F.monotonically_increasing_id().cast("string")
                   ))
        .withColumn("year", F.year(batch_timestamp))
        .withColumn("month", F.month(batch_timestamp))
        .withColumn("day", F.dayofmonth(batch_timestamp))
        .select(
            "match_id", "gameId", "game_datetime", "game_length", "game_version", "tft_game_type", 
            "tft_set_core_name", "tft_set_number",
            "surrogate_key", "is_current", "effective_date", "end_date", "hash_key",
            "processed_ts", "year", "month", "day"
        )
    )
    
    # Cache processed dataframe
    processed_df.cache()
    
    try:
        delta_table = DeltaTable.forPath(spark, silver_data_path)
        
        # Optimization: Only read current records for the specific keys in this batch
        # This reduces the amount of data we need to process
        batch_keys = processed_df.select("gameId").distinct()
        batch_keys.cache()
        
        # Get only current records for the keys in this batch (predicate pushdown)
        current_records = (
            delta_table.toDF()
            .filter(F.col("is_current") == True)
            .join(batch_keys, ["gameId"], "inner")  # Inner join for better performance
            .select(
                "match_id", "gameId", "hash_key", "surrogate_key",
                "effective_date", "is_current"
            )
        )
        current_records.cache()
        
        # Optimized join using broadcast hint for small current_records
        incoming_with_current = (
            processed_df.alias("new")
            .join(
                F.broadcast(current_records.alias("current")),
                (F.col("new.match_id") == F.col("current.match_id")) &
                (F.col("new.gameId") == F.col("current.gameId")),
                "left"
            )
        )
        
        # Identify records that have changed (different hash_key)
        changed_records = (
            incoming_with_current
            .filter(
                (F.col("current.hash_key").isNotNull()) & 
                (F.col("new.hash_key") != F.col("current.hash_key"))
            )
            .select("new.*")
        )
        
        # Identify completely new records (not in current table)
        new_records = (
            incoming_with_current
            .filter(F.col("current.match_id").isNull())
            .select("new.*")
        )
        
        # Combine changed and new records
        records_to_insert = changed_records.union(new_records)
        records_to_insert.cache()
        
        insert_count = records_to_insert.count()
        
        if insert_count > 0:
            print(f"Processing {insert_count} changed/new records")
            
            # Optimization: Batch update operations instead of individual updates
            keys_to_update = records_to_insert.select("match_id", "gameId").distinct()
            
            # Single batch update using merge operation (much faster than individual updates)
            if keys_to_update.count() > 0:
                # Create a temporary view for the update keys
                keys_to_update.createOrReplaceTempView("keys_to_update")
                
                # Batch update using SQL for better performance
                update_condition = """
                target.match_id = source.match_id AND
                target.gameId = source.gameId AND 
                target.is_current = true
                """
                
                (delta_table
                 .alias("target")
                 .merge(
                     keys_to_update.alias("source"),
                     update_condition
                 )
                 .whenMatchedUpdate(set={
                     "is_current": "false",
                     "end_date": "current_timestamp()"
                 })
                 .execute())
            
            # Insert new records
            (delta_table
             .alias("target")
             .merge(
                 records_to_insert.alias("source"),
                 "1=0"  # Always insert (no matching condition)
             )
             .whenNotMatchedInsertAll()
             .execute())
            
            print(f"Successfully processed {insert_count} records")
        else:
            print("No changes detected - skipping updates")
        
        # Cleanup cached dataframes
        batch_keys.unpersist()
        current_records.unpersist()
        records_to_insert.unpersist()
    
    except Exception as e:
        # If table doesn't exist, create it with initial data
        print(f"Creating new silver table: {e}")
        (processed_df
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("year", "month", "day")
         .save(silver_data_path))
        print(f"Created silver table with {processed_df.count()} records")
    
    finally:
        # Always cleanup cached dataframes
        bronze_df.unpersist()
        processed_df.unpersist()

def main():
    spark = create_silver_spark("silver-match-scd2")
    cfg = get_delta_config()
    
    # Apply additional performance optimizations (resource profile already applied)
    apply_performance_config(spark, memory_profile="medium")
    
    # Bronze layer paths
    bronze_layer = "bronze"
    bronze_table_name = "tft_matches"
    bronze_paths = build_paths(bronze_layer, bronze_table_name, cfg)
    
    # Silver layer paths
    silver_layer = "silver"
    silver_table_name = "tft_matches_scd2"
    silver_paths = build_paths(silver_layer, silver_table_name, cfg)
    
    ensure_database(spark, silver_paths.db_name)
    
    # Read from bronze layer as stream
    bronze_stream = read_stream_from_delta(spark, bronze_paths.data_path)
    
    # Select only required columns with optimized column renaming
    selected_df = (
        bronze_stream
        .select(
            "match_id",
            "gameId",
            "game_datetime", 
            "game_length",
            "game_version",
            "tft_game_type",
            "tft_set_core_name",
            "tft_set_number"
        )
        .filter(F.col("gameId").isNotNull())
    )
    
    # Process each micro-batch with SCD Type 2 logic
    def process_batch(batch_df, batch_id):
        batch_count = batch_df.count()
        if batch_count > 0:
            print(f"Processing batch {batch_id} with {batch_count} records")
            
            process_scd2_batch(spark, batch_df, silver_paths.data_path)
            
            print(f"Batch {batch_id} completed successfully")
        else:
            print(f"Batch {batch_id}: No records to process")
    
    # Start the streaming query with optimized settings
    query = (
        selected_df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", silver_paths.checkpoint_path)
        .option("maxFilesPerTrigger", "100")  # Limit files per trigger for better performance
        .trigger(processingTime="60 seconds")  # Increased interval for better batching
        .queryName("silver-match-scd2")
        .start()
    )
    
    # Register the silver table
    register_delta_table(
        spark, 
        silver_paths.db_name, 
        silver_paths.table_name, 
        silver_paths.data_path
    )
    
    print(f"Silver SCD2 table registered: {silver_paths.full_table_name}")
    print(f"Data path: {silver_paths.data_path}")
    print(f"Checkpoint path: {silver_paths.checkpoint_path}")
    
    try:
        print("Starting streaming...")
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        query.stop()

if __name__ == "__main__":
    main()