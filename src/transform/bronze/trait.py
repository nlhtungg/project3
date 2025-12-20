"""
Bronze layer: Load traits data from CSV to Delta table
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp


def get_traits_schema():
    """Define schema for traits CSV"""
    return StructType([
        StructField("set", IntegerType(), True),
        StructField("trait_id", StringType(), True),
        StructField("trait_name", StringType(), True),
        StructField("trait_desc", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("tier1_min", IntegerType(), True),
        StructField("tier1_max", IntegerType(), True),
        StructField("tier1_style", IntegerType(), True),
        StructField("tier2_min", IntegerType(), True),
        StructField("tier2_max", IntegerType(), True),
        StructField("tier2_style", IntegerType(), True),
        StructField("tier3_min", IntegerType(), True),
        StructField("tier3_max", IntegerType(), True),
        StructField("tier3_style", IntegerType(), True),
        StructField("tier4_min", IntegerType(), True),
        StructField("tier4_max", IntegerType(), True),
        StructField("tier4_style", IntegerType(), True),
        StructField("tier5_min", IntegerType(), True),
        StructField("tier5_max", IntegerType(), True),
        StructField("tier5_style", IntegerType(), True),
    ])


def load_traits_to_bronze(spark, source_path, target_path):
    """
    Load traits CSV to Bronze Delta table
    
    Args:
        spark: SparkSession
        source_path: Path to traits CSV file
        target_path: Path to Bronze Delta table
    """
    print(f"Loading traits from {source_path}")
    
    # Read CSV with schema
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(get_traits_schema()) \
        .csv(source_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Show sample
    print(f"Loaded {df.count()} traits")
    df.show(5, truncate=False)
    
    # Write to Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)
    
    print(f"✓ Traits saved to {target_path}")
    
    return df


if __name__ == "__main__":
    # For testing purposes
    spark = SparkSession.builder \
        .appName("Bronze_Traits") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    source = "test_data/traits.csv"
    target = "data/bronze/traits"
    
    load_traits_to_bronze(spark, source, target)
    
    spark.stop()
