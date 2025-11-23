# src/common/io/delta_io.py
from dataclasses import dataclass
from typing import Sequence, Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

from common.config.delta import DeltaLakeConfig, get_delta_config, Layer


@dataclass
class DeltaPaths:
    layer: Layer
    db_name: str
    table_name: str
    full_table_name: str
    data_path: str
    checkpoint_path: str


def build_paths(
    layer: Layer,
    table_name: str,
    cfg: Optional[DeltaLakeConfig] = None,
) -> DeltaPaths:
    """
    Build đầy đủ path cho 1 bảng ở 1 layer:
      - data_path: nơi lưu Delta
      - checkpoint_path: nơi lưu checkpoint stream
      - db_name, full_table_name: cho Hive/Trino
    """
    cfg = cfg or get_delta_config()
    bucket = cfg.get_bucket(layer)
    ck_root = cfg.get_checkpoint_root(layer)
    db_name = cfg.get_db_name(layer)

    data_path = f"{bucket.rstrip('/')}/{table_name}"
    checkpoint_path = f"{ck_root.rstrip('/')}/{table_name}"
    full_table_name = f"{db_name}.{table_name}"

    return DeltaPaths(
        layer=layer,
        db_name=db_name,
        table_name=table_name,
        full_table_name=full_table_name,
        data_path=data_path,
        checkpoint_path=checkpoint_path,
    )


# -------------------- DB & Hive --------------------


def ensure_database(spark: SparkSession, db_name: str) -> None:
    """
    CREATE DATABASE IF NOT EXISTS db_name.
    Dùng cho mọi layer (bronze/silver/gold). :contentReference[oaicite:2]{index=2}
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")


def register_delta_table(
    spark: SparkSession,
    db_name: str,
    table_name: str,
    data_path: str,
    drop_if_exists: bool = True,
) -> None:
    """
    Đăng ký Delta table vào Hive Metastore.
    Dùng chung cho batch & stream. 
    """
    full_table = f"{db_name}.{table_name}"
    if drop_if_exists:
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table}
    USING DELTA
    LOCATION '{data_path}'
    """
    spark.sql(create_sql)


# -------------------- Batch I/O --------------------


def upsert_batch(
    spark: SparkSession,
    df: DataFrame,
    data_path: str,
    merge_condition: str,
) -> None:
    """
    Batch upsert: nếu Delta tồn tại -> MERGE, nếu không -> ghi mới.

    merge_condition ví dụ:
      "target.id = source.id"
    (giống file test.py của bạn) :contentReference[oaicite:4]{index=4}
    """
    try:
        delta_table = DeltaTable.forPath(spark, data_path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(data_path)
        )


def overwrite_batch(
    df: DataFrame,
    data_path: str,
    partition_cols: Optional[Sequence[str]] = None,
) -> None:
    """
    Ghi đè toàn bộ Delta table (không MERGE).
    Hữu ích cho gold layer batch.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(data_path)


# -------------------- Streaming I/O --------------------


def write_stream_to_delta(
    df: DataFrame,
    data_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
    output_mode: str = "append",
    partition_cols: Optional[Sequence[str]] = None,
    query_name: Optional[str] = None,
):
    """
    Ghi streaming DataFrame vào Delta (bronze/silver là chính).

    - data_path: s3a://bronze/table_name
    - checkpoint_path: s3a://bronze/checkpoints/table_name
    - trigger_interval: '10 seconds', '1 minute', ...
    """
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .option("path", data_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger_interval)
    )

    if partition_cols:
        writer = writer.option("partitionBy", ",".join(partition_cols))

    if query_name:
        writer = writer.queryName(query_name)

    query = writer.start()
    return query


def read_stream_from_delta(
    spark: SparkSession,
    data_path: str,
    ignore_changes: bool = True,
    ignore_deletes: bool = True,
):
    """
    Đọc Delta như stream (như file test_read_stream.py). :contentReference[oaicite:5]{index=5}
    """
    return (
        spark.readStream.format("delta")
        .option("ignoreChanges", str(ignore_changes).lower())
        .option("ignoreDeletes", str(ignore_deletes).lower())
        .load(data_path)
    )
