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


def create_match_participant_hash_key(df):
    """Create hash key from changing attributes for comparison"""
    return F.sha2(
        F.concat_ws("||",
            F.coalesce(F.col("placement").cast("string"), F.lit("")),
            F.coalesce(F.col("gold_left").cast("string"), F.lit("")),
            F.coalesce(F.col("last_round").cast("string"), F.lit("")),
            F.coalesce(F.col("level").cast("string"), F.lit("")),
            F.coalesce(F.col("players_eliminated").cast("string"), F.lit("")),
            F.coalesce(F.col("total_damage_to_players").cast("string"), F.lit(""))
        ), 256
    )


def process_match_participant_scd2_batch(spark, bronze_df, silver_data_path):
    """Process SCD Type 2 logic for match participant data"""
    
    if bronze_df.count() == 0:
        return
    
    bronze_df.cache()
    batch_timestamp = F.current_timestamp()
    
    # Prepare the incoming data
    processed_df = (
        bronze_df
        .withColumn("processed_ts", batch_timestamp)
        .withColumn("effective_date", batch_timestamp)
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("hash_key", create_match_participant_hash_key(bronze_df))
        .withColumn("surrogate_key",
                   F.concat(
                       F.col("match_id"),
                       F.lit("_"),
                       F.col("gameId").cast("string"),
                       F.lit("_"),
                       F.col("puuid"),
                       F.lit("_"),
                       F.date_format(batch_timestamp, "yyyyMMddHHmmssSSS"),
                       F.lit("_"),
                       F.monotonically_increasing_id().cast("string")
                   ))
        .withColumn("year", F.year(batch_timestamp))
        .withColumn("month", F.month(batch_timestamp))
        .withColumn("day", F.dayofmonth(batch_timestamp))
        .select(
            "match_id", "gameId", "puuid", "placement", "gold_left", "last_round",
            "level", "players_eliminated", "total_damage_to_players",
            "surrogate_key", "is_current", "effective_date", "end_date", "hash_key",
            "processed_ts", "year", "month", "day"
        )
    )
    
    processed_df.cache()
    
    try:
        delta_table = DeltaTable.forPath(spark, silver_data_path)
        
        # Get batch keys
        batch_keys = processed_df.select("gameId", "puuid").distinct()
        batch_keys.cache()
        
        # Get current records for the keys in this batch
        current_records = (
            delta_table.toDF()
            .filter(F.col("is_current") == True)
            .join(batch_keys, ["gameId", "puuid"], "inner")
            .select(
                "match_id", "gameId", "puuid", "hash_key", "surrogate_key",
                "effective_date", "is_current"
            )
        )
        current_records.cache()
        
        # Join with current records
        incoming_with_current = (
            processed_df.alias("new")
            .join(
                F.broadcast(current_records.alias("current")),
                (F.col("new.match_id") == F.col("current.match_id")) &
                (F.col("new.gameId") == F.col("current.gameId")) &
                (F.col("new.puuid") == F.col("current.puuid")),
                "left"
            )
        )
        
        # Identify changed records
        changed_records = (
            incoming_with_current
            .filter(
                (F.col("current.hash_key").isNotNull()) &
                (F.col("new.hash_key") != F.col("current.hash_key"))
            )
            .select("new.*")
        )
        
        # Identify new records
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
            print(f"Processing {insert_count} changed/new match participant records")
            
            # Get keys to update
            keys_to_update = records_to_insert.select("match_id", "gameId", "puuid").distinct()
            
            if keys_to_update.count() > 0:
                # Batch update existing current records
                update_condition = """
                target.match_id = source.match_id AND
                target.gameId = source.gameId AND
                target.puuid = source.puuid AND
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
                 "1=0"
             )
             .whenNotMatchedInsertAll()
             .execute())
            
            print(f"Successfully processed {insert_count} match participant records")
        else:
            print("No changes detected - skipping updates")
        
        # Cleanup
        batch_keys.unpersist()
        current_records.unpersist()
        records_to_insert.unpersist()
    
    except Exception as e:
        print(f"Creating new silver match participant table: {e}")
        (processed_df
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("year", "month", "day")
         .save(silver_data_path))
        print(f"Created silver table with {processed_df.count()} records")
    
    finally:
        bronze_df.unpersist()
        processed_df.unpersist()


def create_match_participant_silver_stream(spark: SparkSession, cfg=None, max_files_per_trigger=100):
    """Create and return streaming query for TFT match participant silver layer with SCD2.
    
    Args:
        spark: SparkSession to use
        cfg: Delta configuration
        max_files_per_trigger: Maximum number of files to process per trigger (for large bronze tables)
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
    silver_table_name = "tft_match_participants_scd2"
    silver_paths = build_paths(silver_layer, silver_table_name, cfg)
    
    ensure_database(spark, silver_paths.db_name)
    
    # Read from bronze layer as stream
    bronze_stream = read_stream_from_delta(spark, bronze_paths.data_path)
    
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
    
    # Process each micro-batch with SCD Type 2 logic
    def process_batch(batch_df, batch_id):
        batch_count = batch_df.count()
        if batch_count > 0:
            print(f"Processing match participant batch {batch_id} with {batch_count} records")
            process_match_participant_scd2_batch(spark, batch_df, silver_paths.data_path)
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
        .option("maxFilesPerTrigger", str(max_files_per_trigger))
        .trigger(processingTime="60 seconds")
        .queryName("silver-match-participant-scd2")
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
    
    return query


def main():
    spark = create_silver_spark("silver-match-participant-scd2")
    
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
