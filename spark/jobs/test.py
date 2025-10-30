from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ============================================================
# 1️⃣ Khởi tạo SparkSession (đã có config từ spark-defaults.conf & hive-site.xml)
# ============================================================
spark = (
    SparkSession.builder
        .appName("Write Bronze Delta Table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()  # cần cho saveAsTable -> Hive Metastore
        .getOrCreate()
)

# ============================================================
# 2️⃣ Tạo DataFrame mẫu
# ============================================================
data = [
    (1, "Alice", "2025-10-28"),
    (2, "Bob", "2025-10-28"),
    (3, "Charlie Puth", "2025-10-28"),
]
columns = ["id", "name", "ingest_date"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("ingest_ts", F.current_timestamp())

# ============================================================
# 3️⃣ Khai báo database, table, path
# ============================================================
db_name = "bronze"
table_name = "peoples"
full_table_name = f"{db_name}.{table_name}"
delta_path = f"s3a://{db_name}/{table_name}"

# ============================================================
# 4️⃣ Tạo database nếu chưa có (đăng ký trong Hive)
# ============================================================
print(f"📋 Creating database: {db_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print(f"✅ Database created/verified")

# ============================================================
# 5️⃣ Ghi dữ liệu Delta bằng UPSERT (MERGE)
# ============================================================

print(f"📝 Upserting data to Delta table: {delta_path}")

# Check if table exists
try:
    # Table exists - perform MERGE (upsert)
    deltaTable = DeltaTable.forPath(spark, delta_path)
    
    print("🔄 Table exists, performing MERGE (upsert)...")
    (
        deltaTable.alias("target")
        .merge(
            df.alias("source"),
            "target.id = source.id"  # Merge condition (primary key)
        )
        .whenMatchedUpdateAll()  # Update existing records
        .whenNotMatchedInsertAll()  # Insert new records
        .execute()
    )
    print(f"✅ Data upserted successfully!")
    
except Exception as e:
    # Table doesn't exist - create it with initial data
    print(f"📝 Table doesn't exist, creating new table...")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(delta_path)
    )
    print(f"✅ Initial data written successfully!")

print(f"📂 Location: {delta_path}")

# ============================================================
# 6️⃣ Đăng ký bảng trong Hive Metastore bằng CREATE TABLE
# ============================================================
print(f"\n📋 Registering table in Hive Metastore: {full_table_name}")

# Create external table pointing to Delta location
try:
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    print(f"🗑️ Dropped existing table (if any)")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name}
    USING DELTA
    LOCATION '{delta_path}'
    """
    spark.sql(create_table_sql)
    print(f"✅ Table registered: {full_table_name}")
except Exception as e:
    print(f"❌ Error during table registration: {e}")
    raise

# ============================================================
# 7️⃣ Kiểm tra lại đăng ký Hive
# ============================================================
print("\n🔍 Verifying table...")
try:
    spark.sql(f"SHOW TABLES IN {db_name}").show(truncate=False)
    print("\n📊 Querying table via Spark SQL...")
    result = spark.sql(f"SELECT * FROM {full_table_name}")
    result.show(truncate=False)
    print(f"\n✅ SUCCESS! Row count: {result.count()}")
except Exception as e:
    print(f"❌ Table query failed: {e}")
    raise

print("\n✅ DONE! Table is registered in Hive and can be queried via Trino!")

spark.stop()
