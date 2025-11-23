# src/common/config/delta.py
from dataclasses import dataclass
from typing import Literal, Optional
import os

Layer = Literal["bronze", "silver", "gold"]


@dataclass
class DeltaLakeConfig:
    """
    Config Delta Lake / MinIO cho 3 layer: bronze, silver, gold.
    Dùng cho cả batch & stream.
    """

    # DB mặc định (thường là bronze)
    default_db: str = os.getenv("DEFAULT_DELTA_DB", "bronze")

    # Bucket của từng layer
    bronze_bucket: str = os.getenv("BRONZE_BUCKET", "s3a://bronze")
    silver_bucket: str = os.getenv("SILVER_BUCKET", "s3a://silver")
    gold_bucket: str = os.getenv("GOLD_BUCKET", "s3a://gold")

    # Checkpoint root cho từng layer (mặc định: {bucket}/checkpoints)
    bronze_checkpoint_root: str = os.getenv(
        "BRONZE_CHECKPOINT_ROOT", "s3a://bronze/checkpoints"
    )
    silver_checkpoint_root: str = os.getenv(
        "SILVER_CHECKPOINT_ROOT", "s3a://silver/checkpoints"
    )
    gold_checkpoint_root: str = os.getenv(
        "GOLD_CHECKPOINT_ROOT", "s3a://gold/checkpoints"
    )

    # Hive Metastore (có thể None nếu không dùng)
    hive_metastore_uri: Optional[str] = os.getenv("HIVE_METASTORE_URI", None)

    # MinIO / S3A (nếu bạn không set trong spark-defaults.conf)
    s3a_access_key: str = os.getenv("S3A_ACCESS_KEY", "minioadmin")
    s3a_secret_key: str = os.getenv("S3A_SECRET_KEY", "minioadmin")
    s3a_endpoint: str = os.getenv("S3A_ENDPOINT", "http://minio:9000")
    s3a_path_style_access: str = os.getenv("S3A_PATH_STYLE_ACCESS", "true")
    s3a_ssl_enabled: str = os.getenv("S3A_SSL_ENABLED", "false")

    def get_bucket(self, layer: Layer) -> str:
        l = layer.lower()
        if l == "bronze":
            return self.bronze_bucket
        if l == "silver":
            return self.silver_bucket
        if l == "gold":
            return self.gold_bucket
        raise ValueError(f"Unknown layer: {layer}")

    def get_checkpoint_root(self, layer: Layer) -> str:
        l = layer.lower()
        if l == "bronze":
            return self.bronze_checkpoint_root
        if l == "silver":
            return self.silver_checkpoint_root
        if l == "gold":
            return self.gold_checkpoint_root
        raise ValueError(f"Unknown layer: {layer}")

    def get_db_name(self, layer: Layer) -> str:
        """
        Mặc định: db = tên layer (bronze/silver/gold).
        Nếu muốn mapping khác thì sửa hàm này là đủ.
        """
        return layer.lower()


def get_delta_config() -> DeltaLakeConfig:
    return DeltaLakeConfig()
