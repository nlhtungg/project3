import os
from pyspark.sql import SparkSession

aws_access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
aws_secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
s3_endpoint = "http://minio:9000"
bucket_name = os.environ.get("MINIO_BUCKET", "bronze")

def configure_delta_catalog(spark: SparkSession):
    spark.conf.set("spark.sql.catalog.delta", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark.conf.set("spark.sql.catalogImplementation", "hive")
    spark.conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")
    spark.conf.set("spark.sql.warehouse.dir", f"s3a://{bucket_name}/warehouse")

    spark.conf.set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

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
    
    # Hive Metastore configuration (static config - must be set during session creation)
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", f"s3a://{bucket_name}/warehouse")
    
    # S3a configuration for MinIO
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Optimize Delta writes
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    
    # Misc tuning
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession created with Delta Lake + MinIO + Hive Metastore")

# Configure any remaining dynamic settings (if needed in future)
# configure_delta_catalog(spark)  # Currently not needed as all configs are in session builder

# ================== Create database schema ==================
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
print("✅ Created bronze database")

# ================== Ghi Delta thử nghiệm ==================
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eva")]
df = spark.createDataFrame(data, ["id", "name"])

# Register as temporary view first
df.createOrReplaceTempView("temp_people")

print("🚀 Writing to Delta table: bronze.people")

# Define the table location explicitly for Trino compatibility
table_location = f"s3a://{bucket_name}/warehouse/bronze.db/people"

# Create or replace the Delta table with explicit location for Trino
# Using similar pattern as your Iceberg reference but for Delta
spark.sql(f"""
    CREATE OR REPLACE TABLE bronze.people (
        id INT,
        name STRING
    ) USING DELTA
    LOCATION '{table_location}'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'write.format.default' = 'delta'
    )
""")

# Insert data into the table
spark.sql("""
    INSERT OVERWRITE TABLE bronze.people
    SELECT * FROM temp_people
""")

print("✅ Đã ghi thành công vào Delta table bronze.people!")

# Register table in Hive Metastore for Trino visibility
print("📝 Registering table in Hive Metastore for Trino...")
spark.sql("REFRESH TABLE bronze.people")
print("✅ Table registered successfully!")

# ================== Đọc lại để kiểm tra ==================
print("📊 Delta Table contents from bronze.people:")
result_df = spark.sql("SELECT * FROM bronze.people")
result_df.show()

# Also show table info
print("📋 Table information:")
spark.sql("DESCRIBE EXTENDED bronze.people").show(truncate=False)

spark.stop()
