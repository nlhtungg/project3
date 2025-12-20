"""
Bronze layer: Load augments data from CSV to Delta table
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp


def get_augments_schema():
    """Define schema for augments CSV"""
    return StructType([
        StructField("set", IntegerType(), True),
        StructField("augment_id", StringType(), True),
        StructField("augment_name", StringType(), True),
        StructField("augment_desc", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("associated_traits", StringType(), True),
    ])


def load_augments_to_bronze(spark, source_path, target_path):
    """
    Load augments CSV to Bronze Delta table
    
    Args:
        spark: SparkSession
        source_path: Path to augments CSV file
        target_path: Path to Bronze Delta table
    """
    print(f"Loading augments from {source_path}")
    
    # Read CSV with schema
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(get_augments_schema()) \
        .csv(source_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Show sample
    print(f"Loaded {df.count()} augments")
    df.show(5, truncate=False)
    
    # Write to Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)
    
    print(f"✓ Augments saved to {target_path}")
    
    return df


if __name__ == "__main__":
    # For testing purposes
    spark = SparkSession.builder \
        .appName("Bronze_Augments") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    source = "test_data/augments.csv"
    target = "data/bronze/augments"
    
    load_augments_to_bronze(spark, source, target)
    
    spark.stop()
