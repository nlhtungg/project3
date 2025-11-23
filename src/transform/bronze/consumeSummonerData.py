import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from delta.tables import DeltaTable
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# Configuration - Hardcoded values
# ============================================================
# Kafka Configuration
kafka_bootstrap_servers = "kafka-1:9092,kafka-2:9092"
kafka_topic = "tft_summoner"
consumer_group_id = "summoner-bronze-consumer"

# Delta Lake Configuration
db_name = "bronze"
table_name = "tft_summoners"
full_table_name = f"{db_name}.{table_name}"
bronze_bucket_path = f"s3a://{db_name}/{table_name}"
checkpoint_location = f"s3a://{db_name}/checkpoints/{table_name}"

# Define schema for summoner data
summoner_schema = StructType([
    StructField("puuid", StringType(), True),
    StructField("region", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("rank", StringType(), True),
    StructField("leaguePoints", IntegerType(), True),
    StructField("wins", IntegerType(), True),
    StructField("losses", IntegerType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("processed_ts", TimestampType(), True)
])

class SummonerDataConsumer:
    def __init__(self):
        self.spark = None
        self.streaming_query = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        if self.streaming_query and self.streaming_query.isActive:
            self.streaming_query.stop()
        
    def _init_spark(self):
        """Initialize Spark session with Delta Lake and Kafka support"""
        try:
            # ============================================================
            # Khởi tạo SparkSession (packages loaded via spark-submit)
            # ============================================================
            self.spark = (
                SparkSession.builder
                    .appName("TFT-Summoner-Bronze-Consumer")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                    .config("spark.databricks.delta.vacuum.logging.enabled", "true")
                    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
                    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.cores", "2")
                    .config("spark.executor.cores", "2")
                    .config("spark.executor.instances", "1")
                    .config("spark.cores.max", "2")
                    # MinIO/S3 configurations - hardcoded
                    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                    .enableHiveSupport()
                    .getOrCreate()
            )
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
            
    def run(self):
        """Main streaming consumer using Spark Structured Streaming"""
        try:
            # ============================================================
            # 1️⃣ Initialize Spark session
            # ============================================================
            self._init_spark()
            
            # ============================================================
            # 2️⃣ Create database if not exists
            # ============================================================
            print(f"📋 Creating database: {db_name}")
            hive_available = True
            try:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                logger.info(f"Database {db_name} created/verified in Hive Metastore")
            except Exception as e:
                logger.warning(f"Hive Metastore unavailable: {e}")
                logger.info(f"Will write to Delta catalog only (path: {bronze_bucket_path})")
                hive_available = False
            
            # ============================================================
            # 3️⃣ Define the JSON schema for summoner data
            # ============================================================
            summoner_json_schema = StructType([
                StructField("puuid", StringType(), True),
                StructField("region", StringType(), True),
                StructField("tier", StringType(), True),
                StructField("rank", StringType(), True),
                StructField("leaguePoints", IntegerType(), True),
                StructField("wins", IntegerType(), True),
                StructField("losses", IntegerType(), True),
                StructField("ingest_ts", StringType(), True)
            ])
            
            # Read from Kafka using Spark Structured Streaming
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("subscribe", kafka_topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.consumer.group.id", consumer_group_id) \
                .load()
            
            # Parse Kafka messages
            parsed_df = kafka_df.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("message_value"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Parse JSON and extract summoner data
            summoner_df = parsed_df.select(
                col("message_key"),
                col("kafka_timestamp"),
                from_json(col("message_value"), summoner_json_schema).alias("summoner_data")
            ).select(
                col("message_key"),
                col("kafka_timestamp"),
                col("summoner_data.puuid").alias("puuid"),
                col("summoner_data.region").alias("region"),
                col("summoner_data.tier").alias("tier"),
                col("summoner_data.rank").alias("rank"),
                col("summoner_data.leaguePoints").alias("leaguePoints"),
                col("summoner_data.wins").alias("wins"),
                col("summoner_data.losses").alias("losses"),
                col("summoner_data.ingest_ts").alias("ingest_ts")
            )
            
            # Add processing timestamp and partition columns
            enriched_df = summoner_df \
                .withColumn("processed_ts", current_timestamp()) \
                .withColumn("year", expr("year(processed_ts)")) \
                .withColumn("month", expr("month(processed_ts)")) \
                .withColumn("day", expr("dayofmonth(processed_ts)"))
            
            # Filter out null/empty puuid records
            final_df = enriched_df.filter(col("puuid").isNotNull() & (col("puuid") != ""))
            
            # ============================================================
            # 6️⃣ Initialize Delta table structure if it doesn't exist
            # ============================================================
            try:
                # Check if Delta table exists
                delta_table = DeltaTable.forPath(self.spark, bronze_bucket_path)
                logger.info(f"📋 Delta table exists at: {bronze_bucket_path}")
            except Exception:
                # Table doesn't exist - create it with proper schema and partitioning
                logger.info(f"📝 Creating new Delta table with partitioning: {bronze_bucket_path}")
                
                # Create a sample dataframe with the schema to initialize the table
                sample_data = [(
                    "sample_key", None, "sample_puuid", "NA", "IRON", "IV", 0, 0, 0, 
                    "2025-01-01T00:00:00Z", None, 2025, 1, 1
                )]
                sample_columns = [
                    "message_key", "kafka_timestamp", "puuid", "region", "tier", "rank", 
                    "leaguePoints", "wins", "losses", "ingest_ts", "processed_ts", "year", "month", "day"
                ]
                
                sample_df = self.spark.createDataFrame(sample_data, sample_columns)
                sample_df = sample_df.withColumn("processed_ts", current_timestamp())
                
                # Write initial structure with partitioning
                sample_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .partitionBy("year", "month", "day", "tier") \
                    .save(bronze_bucket_path)
                
                # Remove the sample data
                self.spark.sql(f"DELETE FROM delta.`{bronze_bucket_path}` WHERE puuid = 'sample_puuid'")
                logger.info(f"✅ Delta table structure created with partitioning")
            
            # ============================================================
            # 7️⃣ Write streaming data to Delta Lake
            # ============================================================
            logger.info(f"📝 Writing streaming data to Delta table: {bronze_bucket_path}")
            logger.info(f"📍 Checkpoint location: {checkpoint_location}")
            logger.info(f"⏱️  Trigger interval: 30 seconds")
            logger.info("=" * 60)
            
            self.streaming_query = final_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_location) \
                .option("path", bronze_bucket_path) \
                .option("mergeSchema", "true") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info(f"✅ Started streaming from Kafka topic '{kafka_topic}' to Delta table at '{bronze_bucket_path}'")
            logger.info(f"📊 Query ID: {self.streaming_query.id}")
            logger.info(f"📌 Query Name: {self.streaming_query.name if self.streaming_query.name else 'Unnamed'}")
            logger.info("=" * 60)
            
            # Wait for the streaming to finish
            self.streaming_query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("⚠️  Consumer interrupted by user")
        except Exception as e:
            logger.error(f"💥 Fatal error in consumer: {e}")
            raise
        finally:
            # ============================================================
            # 8️⃣ Register table in Hive Metastore (if available)
            # ============================================================
            if hasattr(self, 'spark') and self.spark:
                try:
                    logger.info(f"📋 Registering table in Hive Metastore: {full_table_name}")
                    self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {full_table_name}
                    USING DELTA
                    LOCATION '{bronze_bucket_path}'
                    """
                    self.spark.sql(create_table_sql)
                    logger.info(f"✅ Table registered: {full_table_name}")
                except Exception as e:
                    logger.warning(f"⚠️  Could not register table in Hive Metastore: {e}")
                    logger.info(f"📝 Data is safe in Delta Lake at: {bronze_bucket_path}")
            
            self._cleanup()
            
    def _cleanup(self):
        """Clean up resources"""
        try:
            if self.streaming_query and self.streaming_query.isActive:
                logger.info("⏹️  Stopping streaming query...")
                self.streaming_query.stop()
                self.streaming_query.awaitTermination(timeout=10)
                logger.info("✅ Streaming query stopped")
                
            if self.spark:
                logger.info("🛑 Stopping Spark session...")
                self.spark.stop()
                logger.info("✅ Spark session stopped")
                
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")

def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("🚀 Starting TFT Summoner Data Consumer")
    logger.info(f"📂 Delta table location: {bronze_bucket_path}")
    logger.info(f"📍 Checkpoint location: {checkpoint_location}")
    logger.info(f"🗃️  Hive table: {full_table_name}")
    logger.info("=" * 60)
    
    consumer = SummonerDataConsumer()
    try:
        consumer.run()
    except Exception as e:
        logger.error(f"💥 Consumer failed: {e}")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("✅ TFT SUMMONER CONSUMER COMPLETE!")
    logger.info(f"📂 Delta table location: {bronze_bucket_path}")
    logger.info(f"🗃️  Hive table: {full_table_name}")
    logger.info("💡 You can query this table from Trino or Spark SQL")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()