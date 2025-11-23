# src/common/io/kafka_stream.py
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

from common.config.kafka import get_kafka_config


def read_kafka_stream(
    spark: SparkSession,
    topic: str,
    group_id: str,
    starting_offsets: str = "earliest",
    fail_on_data_loss: bool = False,
) -> DataFrame:
    """
    Tạo DataFrame streaming từ Kafka (chung cho mọi job bronze/silver). :contentReference[oaicite:6]{index=6}
    """
    kafka_cfg = get_kafka_config()
    opts = kafka_cfg.spark_options()

    reader = spark.readStream.format("kafka")
    for k, v in opts.items():
        reader = reader.option(k, v)

    reader = (
        reader.option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", str(fail_on_data_loss).lower())
        .option("kafka.consumer.group.id", group_id)
    )

    return reader.load()


def parse_kafka_json(
    kafka_df: DataFrame,
    json_schema: StructType,
    key_col: str = "message_key",
    value_col: str = "message_value",
    keep_kafka_ts: bool = True,
) -> DataFrame:
    """
    Parse Kafka (key, value, timestamp) → cột JSON flatten.

    - key, value: cast string
    - dùng from_json để parse payload như trong summoner.py. :contentReference[oaicite:7]{index=7}
    """
    df = kafka_df.select(
        col("key").cast("string").alias(key_col),
        col("value").cast("string").alias(value_col),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed = df.select(
        col(key_col),
        col("kafka_timestamp"),
        from_json(col(value_col), json_schema).alias("data"),
    )

    cols = [key_col]
    if keep_kafka_ts:
        cols.append("kafka_timestamp")
    cols.extend([f"data.{f.name}" for f in json_schema.fields])

    return parsed.select(*cols)
