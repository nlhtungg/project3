"""
Bronze layer: Load units data from CSV to Delta table
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp
import os


def get_units_schema():
    """Define schema for units CSV"""
    return StructType([
        StructField("set", IntegerType(), True),
        StructField("unit_id", StringType(), True),
        StructField("unit_name", StringType(), True),
        StructField("cost", IntegerType(), True),
        StructField("trait1", StringType(), True),
        StructField("trait2", StringType(), True),
        StructField("trait3", StringType(), True),
        StructField("trait4", StringType(), True),
        StructField("num_traits", IntegerType(), True),
        StructField("all_traits", StringType(), True),
        StructField("ability_name", StringType(), True),
        StructField("ability_desc", StringType(), True),
        StructField("health", DoubleType(), True),
        StructField("armor", DoubleType(), True),
        StructField("magic_resist", DoubleType(), True),
        StructField("attack_damage", DoubleType(), True),
        StructField("attack_speed", DoubleType(), True),
        StructField("attack_range", DoubleType(), True),
        StructField("mana_start", DoubleType(), True),
        StructField("mana_max", DoubleType(), True),
        StructField("crit_chance", DoubleType(), True),
        StructField("crit_multiplier", DoubleType(), True),
    ])


def load_units_to_bronze(spark, source_path, target_path):
    """
    Load units CSV to Bronze Delta table
    
    Args:
        spark: SparkSession
        source_path: Path to units CSV file
        target_path: Path to Bronze Delta table
    """
    print(f"Loading units from {source_path}")
    
    # Read CSV with schema
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(get_units_schema()) \
        .csv(source_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Show sample
    print(f"Loaded {df.count()} units")
    df.show(5, truncate=False)
    
    # Write to Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)
    
    print(f"✓ Units saved to {target_path}")
    
    return df


if __name__ == "__main__":
    # For testing purposes
    spark = SparkSession.builder \
        .appName("Bronze_Units") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    source = "test_data/units.csv"
    target = "data/bronze/units"
    
    load_units_to_bronze(spark, source, target)
    
    spark.stop()
