from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time

# ============================================================
# 1️⃣ Khởi tạo SparkSession với Delta Lake support
# ============================================================
spark = (
    SparkSession.builder
        .appName("Delta Lake Streaming Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", "s3a://bronze/checkpoints")
        .enableHiveSupport()
        .getOrCreate()
)

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2️⃣ Define schema for streaming data
# ============================================================
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# ============================================================
# 3️⃣ Create streaming DataFrame using rate source (generates data)
# ============================================================
print("🚀 Starting streaming job...")
print("📊 Generating streaming data using rate source...")

# Rate source generates data at specified rate (rows per second)
streaming_df = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)  # Generate 5 rows per second
        .load()
        .withColumn("id", F.col("value").cast("integer"))
        .withColumn("name", F.concat(F.lit("User_"), F.col("value")))
        .withColumn("event_type", 
                   F.when(F.col("value") % 3 == 0, "purchase")
                    .when(F.col("value") % 3 == 1, "view")
                    .otherwise("click"))
        .withColumn("amount", (F.rand() * 1000).cast("integer"))
        .withColumn("ingest_ts", F.col("timestamp"))
        .select("id", "name", "event_type", "amount", "ingest_ts")
)

# ============================================================
# 4️⃣ Define Delta Lake table location
# ============================================================
db_name = "bronze"
table_name = "streaming_events"
full_table_name = f"{db_name}.{table_name}"
delta_path = f"s3a://{db_name}/{table_name}"
checkpoint_path = f"s3a://{db_name}/checkpoints/{table_name}"

# ============================================================
# 5️⃣ Create database if not exists
# ============================================================
print(f"📋 Creating database: {db_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# ============================================================
# 6️⃣ Write streaming data to Delta Lake
# ============================================================
print(f"📝 Writing streaming data to Delta table: {delta_path}")
print(f"📍 Checkpoint location: {checkpoint_path}")
print(f"⏱️  Trigger interval: 10 seconds")
print("=" * 60)

query = (
    streaming_df.writeStream
        .format("delta")
        .outputMode("append")  # For streaming, use append mode
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")  # Micro-batch every 10 seconds
        .option("path", delta_path)
        .start()
)

# ============================================================
# 7️⃣ Monitor streaming query
# ============================================================
print("✅ Streaming query started!")
print("📊 Query ID:", query.id)
print("📌 Query Name:", query.name if query.name else "Unnamed")
print("=" * 60)

# Run for 60 seconds to collect some data
duration = 60
print(f"⏳ Running streaming for {duration} seconds...")
print("💡 You can stop early with Ctrl+C")
print("=" * 60)

try:
    for i in range(duration):
        time.sleep(1)
        if i % 10 == 0 and i > 0:
            status = query.status
            print(f"\n📈 Progress after {i} seconds:")
            print(f"   - Is Active: {query.isActive}")
            print(f"   - Message: {status['message']}")
            if 'numInputRows' in status:
                print(f"   - Input Rows: {status['numInputRows']}")
            
    print("\n⏹️  Stopping streaming query...")
    query.stop()
    query.awaitTermination(timeout=10)
    
except KeyboardInterrupt:
    print("\n⚠️  Interrupted by user. Stopping query...")
    query.stop()
    query.awaitTermination(timeout=10)

# ============================================================
# 8️⃣ Register table in Hive Metastore
# ============================================================
print(f"\n📋 Registering table in Hive Metastore: {full_table_name}")

try:
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name}
    USING DELTA
    LOCATION '{delta_path}'
    """
    spark.sql(create_table_sql)
    print(f"✅ Table registered: {full_table_name}")
except Exception as e:
    print(f"❌ Error during table registration: {e}")

# ============================================================
# 9️⃣ Verify data was written
# ============================================================
print("\n🔍 Verifying written data...")
try:
    result = spark.sql(f"SELECT * FROM {full_table_name} ORDER BY id DESC LIMIT 10")
    print(f"\n📊 Latest 10 records in {full_table_name}:")
    result.show(truncate=False)
    
    count_df = spark.sql(f"SELECT COUNT(*) as total_records FROM {full_table_name}")
    total = count_df.collect()[0]['total_records']
    print(f"\n✅ SUCCESS! Total records in table: {total}")
    
    # Show event type distribution
    print(f"\n📈 Event type distribution:")
    spark.sql(f"""
        SELECT event_type, COUNT(*) as count 
        FROM {full_table_name} 
        GROUP BY event_type
        ORDER BY count DESC
    """).show()
    
except Exception as e:
    print(f"❌ Table query failed: {e}")

print("\n" + "=" * 60)
print("✅ STREAMING TEST COMPLETE!")
print(f"📂 Delta table location: {delta_path}")
print(f"📍 Checkpoint location: {checkpoint_path}")
print(f"🗃️  Hive table: {full_table_name}")
print("💡 You can query this table from Trino or Spark SQL")
print("=" * 60)

spark.stop()
