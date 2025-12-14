from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_bronze_spark
from common.io.kafka_stream import read_kafka_stream, parse_kafka_json
from common.io.delta_io import build_paths, ensure_database, write_stream_to_delta, register_delta_table
from common.config.delta import get_delta_config

# Schema for participant unit Kafka message
participant_unit_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("gameId", IntegerType(), True),
    StructField("puuid", StringType(), True),
    StructField("character_id", StringType(), True),
    StructField("tier", IntegerType(), True),
    StructField("item1", StringType(), True),
    StructField("item2", StringType(), True),
    StructField("item3", StringType(), True),
])

def create_participant_unit_stream(spark: SparkSession, cfg=None):
    """Create and return streaming query for TFT participant units."""
    if cfg is None:
        cfg = get_delta_config()
    
    layer = "bronze"
    table_name = "tft_participant_units"
    paths = build_paths(layer, table_name, cfg)
    
    ensure_database(spark, paths.db_name)
    
    kafka_df = read_kafka_stream(
        spark,
        topic="tft_participant_unit",
        group_id="participant-unit-bronze-consumer",
    )
    
    # Parse Kafka message and include the message key
    parsed_df = parse_kafka_json(kafka_df, participant_unit_schema)
    
    # Add message key and processing metadata
    df = (
        parsed_df
        .withColumn("processed_ts", F.current_timestamp())
        .withColumn("year", F.year("processed_ts"))
        .withColumn("month", F.month("processed_ts"))
        .withColumn("day", F.dayofmonth("processed_ts"))
    )
    
    query = write_stream_to_delta(
        df,
        data_path=paths.data_path,
        checkpoint_path=paths.checkpoint_path,
        trigger_interval="30 seconds",
    )
    
    register_delta_table(spark, paths.db_name, paths.table_name, paths.data_path)
    return query

if __name__ == "__main__":
    spark = create_bronze_spark("bronze-participant-unit-stream")
    query = create_participant_unit_stream(spark)
    query.awaitTermination()
