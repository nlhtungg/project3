import os
from pyspark.sql import SparkSession

aws_access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
aws_secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
s3_endpoint = "http://minio:9000"
bucket_name = os.environ.get("MINIO_BUCKET", "bronze")

print("🔑 Cấu hình kết nối MinIO cho Spark:", {
    "aws_access_key": aws_access_key,
    "aws_secret_key": aws_secret_key,
    "s3_endpoint": s3_endpoint,
    "bucket_name": bucket_name
})

# ================== Tạo SparkSession ==================
spark = (
    SparkSession.builder
    .appName("DeltaLakeTestWrite")
    .master("spark://spark-master:7077")
    # Delta Lake config
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Optimize Delta writes
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    # Misc tuning
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession created with Delta Lake + MinIO")

# ================== Ghi Delta thử nghiệm ==================
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eva")]
df = spark.createDataFrame(data, ["id", "name"])

# Đường dẫn Delta Table trên MinIO
delta_path = f"s3a://{bucket_name}/test_people_delta"

print(f"🚀 Writing Delta table to: {delta_path}")
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(delta_path)
)

print("✅ Đã ghi thành công Delta Table!")

# ================== Đọc lại để kiểm tra ==================
read_df = spark.read.format("delta").load(delta_path)
print("📊 Delta Table contents:")
read_df.show()

spark.stop()
