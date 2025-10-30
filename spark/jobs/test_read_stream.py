from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

# ============================================================
# 1️⃣ Khởi tạo SparkSession với Delta Lake support
# ============================================================
spark = (
    SparkSession.builder
        .appName("Delta Lake Read Stream Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", "s3a://bronze/checkpoints")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.cores", "2")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "1")
        .config("spark.cores.max", "2")
        .enableHiveSupport()
        .getOrCreate()
)

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2️⃣ Define source and target paths
# ============================================================
source_db = "bronze"
source_table = "streaming_events"
source_delta_path = f"s3a://{source_db}/{source_table}"

target_db = "bronze"
target_table = "streaming_events_processed"
target_delta_path = f"s3a://{target_db}/{target_table}"
checkpoint_path = f"s3a://{target_db}/checkpoints/{target_table}"

print("=" * 60)
print("🚀 Starting Delta Lake Streaming Read Test")
print("=" * 60)
print(f"📖 Source: {source_delta_path}")
print(f"📝 Target: {target_delta_path}")
print(f"📍 Checkpoint: {checkpoint_path}")
print("=" * 60)

# ============================================================
# 3️⃣ Read streaming data from Delta Lake
# ============================================================
print(f"\n📖 Reading stream from Delta table: {source_delta_path}")

streaming_read_df = (
    spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")  # Important for reading from Delta
        .option("ignoreDeletes", "true")  # Ignore delete operations
        .load(source_delta_path)
)

print("✅ Stream reading configured")
print(f"📊 Schema:")
streaming_read_df.printSchema()

# ============================================================
# 4️⃣ Apply transformations on streaming data
# ============================================================
print("\n🔄 Applying transformations...")

# Add aggregations and transformations
processed_df = (
    streaming_read_df
        # Add processing timestamp
        .withColumn("processing_ts", F.current_timestamp())
        
        # Calculate revenue tier based on amount
        .withColumn("revenue_tier", 
                   F.when(F.col("amount") < 200, "low")
                    .when((F.col("amount") >= 200) & (F.col("amount") < 600), "medium")
                    .when((F.col("amount") >= 600) & (F.col("amount") < 900), "high")
                    .otherwise("premium"))
        
        # Add day of week
        .withColumn("day_of_week", F.dayofweek(F.col("ingest_ts")))
        
        # Add hour of day
        .withColumn("hour_of_day", F.hour(F.col("ingest_ts")))
        
        # Calculate time difference
        .withColumn("processing_delay_seconds", 
                   F.unix_timestamp(F.col("processing_ts")) - 
                   F.unix_timestamp(F.col("ingest_ts")))
)

print("✅ Transformations configured:")
print("   - Added processing_ts")
print("   - Added revenue_tier categorization")
print("   - Added day_of_week and hour_of_day")
print("   - Calculated processing_delay_seconds")

# ============================================================
# 5️⃣ Create database if not exists
# ============================================================
print(f"\n📋 Creating database: {target_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

# ============================================================
# 6️⃣ Write processed stream to another Delta table
# ============================================================
print(f"\n📝 Writing processed stream to: {target_delta_path}")
print(f"⏱️  Trigger interval: 15 seconds")
print("=" * 60)

query = (
    processed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="15 seconds")
        .option("path", target_delta_path)
        .start()
)

# ============================================================
# 7️⃣ Also write to console for monitoring
# ============================================================
print("\n📺 Starting console output stream (for monitoring)...")

console_query = (
    processed_df
        .select("id", "name", "event_type", "amount", "revenue_tier", "processing_delay_seconds")
        .writeStream
        .format("console")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .option("truncate", "false")
        .start()
)

# ============================================================
# 8️⃣ Monitor streaming queries
# ============================================================
print("✅ Streaming queries started!")
print("\n📊 Main Query:")
print(f"   - Query ID: {query.id}")
print(f"   - Is Active: {query.isActive}")
print("\n📺 Console Query:")
print(f"   - Query ID: {console_query.id}")
print(f"   - Is Active: {console_query.isActive}")
print("=" * 60)

# Run for 90 seconds
duration = 90
print(f"\n⏳ Running streaming for {duration} seconds...")
print("💡 You can stop early with Ctrl+C")
print("=" * 60)

try:
    for i in range(duration):
        time.sleep(1)
        if i % 15 == 0 and i > 0:
            status = query.status
            console_status = console_query.status
            
            print(f"\n📈 Progress after {i} seconds:")
            print(f"   Main Query:")
            print(f"   - Is Active: {query.isActive}")
            print(f"   - Message: {status['message']}")
            
            print(f"   Console Query:")
            print(f"   - Is Active: {console_query.isActive}")
            print(f"   - Message: {console_status['message']}")
            print("=" * 60)
    
    print("\n⏹️  Stopping streaming queries...")
    query.stop()
    console_query.stop()
    query.awaitTermination(timeout=10)
    console_query.awaitTermination(timeout=10)
    
except KeyboardInterrupt:
    print("\n⚠️  Interrupted by user. Stopping queries...")
    query.stop()
    console_query.stop()
    query.awaitTermination(timeout=10)
    console_query.awaitTermination(timeout=10)

# ============================================================
# 9️⃣ Register table in Hive Metastore
# ============================================================
full_target_table = f"{target_db}.{target_table}"
print(f"\n📋 Registering table in Hive Metastore: {full_target_table}")

try:
    spark.sql(f"DROP TABLE IF EXISTS {full_target_table}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_target_table}
    USING DELTA
    LOCATION '{target_delta_path}'
    """
    spark.sql(create_table_sql)
    print(f"✅ Table registered: {full_target_table}")
except Exception as e:
    print(f"❌ Error during table registration: {e}")

# ============================================================
# 🔟 Verify processed data
# ============================================================
print("\n🔍 Verifying processed data...")
try:
    result = spark.sql(f"SELECT * FROM {full_target_table} ORDER BY id DESC LIMIT 10")
    print(f"\n📊 Latest 10 processed records:")
    result.show(truncate=False)
    
    count_df = spark.sql(f"SELECT COUNT(*) as total_records FROM {full_target_table}")
    total = count_df.collect()[0]['total_records']
    print(f"\n✅ Total processed records: {total}")
    
    # Show revenue tier distribution
    print(f"\n💰 Revenue tier distribution:")
    spark.sql(f"""
        SELECT revenue_tier, COUNT(*) as count, AVG(amount) as avg_amount
        FROM {full_target_table} 
        GROUP BY revenue_tier
        ORDER BY avg_amount DESC
    """).show()
    
    # Show event type breakdown by revenue tier
    print(f"\n📈 Event type by revenue tier:")
    spark.sql(f"""
        SELECT event_type, revenue_tier, COUNT(*) as count
        FROM {full_target_table} 
        GROUP BY event_type, revenue_tier
        ORDER BY event_type, revenue_tier
    """).show()
    
    # Show average processing delay
    print(f"\n⏱️  Processing delay statistics:")
    spark.sql(f"""
        SELECT 
            AVG(processing_delay_seconds) as avg_delay,
            MIN(processing_delay_seconds) as min_delay,
            MAX(processing_delay_seconds) as max_delay
        FROM {full_target_table}
    """).show()
    
except Exception as e:
    print(f"❌ Table query failed: {e}")

print("\n" + "=" * 60)
print("✅ STREAMING READ TEST COMPLETE!")
print("=" * 60)
print(f"📖 Source table: {source_db}.{source_table}")
print(f"📝 Target table: {full_target_table}")
print(f"📂 Delta location: {target_delta_path}")
print(f"📍 Checkpoint: {checkpoint_path}")
print("\n💡 Key Features Demonstrated:")
print("   ✅ Read streaming data from Delta Lake")
print("   ✅ Apply transformations on streaming data")
print("   ✅ Write to another Delta table")
print("   ✅ Console output for monitoring")
print("   ✅ Revenue tier categorization")
print("   ✅ Processing delay calculation")
print("=" * 60)

spark.stop()
