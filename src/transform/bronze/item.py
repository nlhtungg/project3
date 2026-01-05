"""
Bronze layer: Load items data from CSV to Delta table with row-based attributes
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, MapType
from pyspark.sql.functions import col, current_timestamp, from_json, explode, expr


def get_items_schema():
    """Define schema for items CSV"""
    return StructType([
        StructField("set", IntegerType(), True),
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_desc", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("component1", StringType(), True),
        StructField("component2", StringType(), True),
        StructField("num_components", IntegerType(), True),
        StructField("unique", BooleanType(), True),
        StructField("effects", StringType(), True),  # JSON string
    ])


def load_items_to_bronze(spark, source_path, target_path):
    """
    Load items CSV to Bronze Delta table with row-based attributes.
    Each item attribute becomes a separate row for flexible, dynamic schema.
    
    Output schema:
    - set, item_id, item_name, item_desc, icon, component1, component2, num_components, unique
    - attribute_name: The name of the attribute (e.g., "AD", "AP", "Health")
    - attribute_value: The numeric value of the attribute
    - ingestion_timestamp
    
    Args:
        spark: SparkSession
        source_path: Path to items CSV file
        target_path: Path to Bronze Delta table
    """
    print(f"Loading items from {source_path}")
    
    # Read CSV with schema
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("escape", '"') \
        .schema(get_items_schema()) \
        .csv(source_path)
    
    # Debug: show raw effects values
    print("Sample effects values:")
    df.select("item_name", "effects").show(3, truncate=False)
    
    # Parse effects JSON into map
    effects_schema = MapType(StringType(), StringType())
    
    df = df.withColumn(
        "effects_map",
        from_json(col("effects"), effects_schema)
    )
    
    # Drop the original effects column
    df = df.drop("effects")
    
    # Explode the map into rows: each attribute becomes a separate row
    # This creates one row per item per attribute
    df_exploded = df.select(
        col("set"),
        col("item_id"),
        col("item_name"),
        col("item_desc"),
        col("icon"),
        col("component1"),
        col("component2"),
        col("num_components"),
        col("unique"),
        explode(col("effects_map")).alias("attribute_name", "attribute_value")
    )
    
    # Cast attribute_value to double (handle nulls gracefully)
    df_exploded = df_exploded.withColumn(
        "attribute_value",
        expr("CAST(attribute_value AS DOUBLE)")
    )
    
    # Add metadata columns
    df_exploded = df_exploded.withColumn("ingestion_timestamp", current_timestamp())
    
    # Show sample
    print(f"Loaded {df_exploded.count()} item-attribute rows")
    print("Sample data (row-based attributes):")
    df_exploded.filter(col("item_name") == df_exploded.select("item_name").first()[0]) \
        .show(10, truncate=False)
    
    # Show attribute distribution
    print("Attribute distribution:")
    df_exploded.groupBy("attribute_name").count().orderBy(col("count").desc()).show()
    
    # Write to Delta
    df_exploded.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)
    
    print(f"✓ Items saved to {target_path} in row-based format")
    
    return df_exploded


if __name__ == "__main__":
    # For testing purposes
    spark = SparkSession.builder \
        .appName("Bronze_Items") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    source = "test_data/items.csv"
    target = "data/bronze/items"
    
    load_items_to_bronze(spark, source, target)
    
    spark.stop()
