from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_spark
from common.io.kafka_stream import read_kafka_stream, parse_kafka_json
from common.io.delta_io import build_paths, ensure_database, write_stream_to_delta, register_delta_table
from common.config.delta import get_delta_config

spark = create_spark("bronze-summoner-stream")
cfg = get_delta_config()

layer = "bronze"
table_name = "tft_summoners"
paths = build_paths(layer, table_name, cfg)

ensure_database(spark, paths.db_name)

# schema JSON của message
summoner_schema = StructType([
    StructField("puuid", StringType(), True),
    StructField("region", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("rank", StringType(), True),
    StructField("leaguePoints", IntegerType(), True),
    StructField("wins", IntegerType(), True),
    StructField("losses", IntegerType(), True),
    StructField("ingest_ts", StringType(), True),
])

kafka_df = read_kafka_stream(
    spark,
    topic="tft_summoner",
    group_id="summoner-bronze-consumer",
)
parsed_df = parse_kafka_json(kafka_df, summoner_schema)
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
query.awaitTermination()
