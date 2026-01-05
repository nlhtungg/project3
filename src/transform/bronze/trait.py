"""
Bronze layer: Load traits data from CSV to Delta table with row-based tiers
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp, expr, array, struct, explode, lit


def get_traits_schema():
    """Define schema for traits CSV - supports up to 12 tiers"""
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
        StructField("tier6_min", IntegerType(), True),
        StructField("tier6_max", IntegerType(), True),
        StructField("tier6_style", IntegerType(), True),
        StructField("tier7_min", IntegerType(), True),
        StructField("tier7_max", IntegerType(), True),
        StructField("tier7_style", IntegerType(), True),
        StructField("tier8_min", IntegerType(), True),
        StructField("tier8_max", IntegerType(), True),
        StructField("tier8_style", IntegerType(), True),
        StructField("tier9_min", IntegerType(), True),
        StructField("tier9_max", IntegerType(), True),
        StructField("tier9_style", IntegerType(), True),
        StructField("tier10_min", IntegerType(), True),
        StructField("tier10_max", IntegerType(), True),
        StructField("tier10_style", IntegerType(), True),
        StructField("tier11_min", IntegerType(), True),
        StructField("tier11_max", IntegerType(), True),
        StructField("tier11_style", IntegerType(), True),
        StructField("tier12_min", IntegerType(), True),
        StructField("tier12_max", IntegerType(), True),
        StructField("tier12_style", IntegerType(), True),
    ])


def load_traits_to_bronze(spark, source_path, target_path):
    """
    Load traits CSV to Bronze Delta table with row-based tier structure.
    Each tier becomes a separate row for flexible, dynamic schema.
    
    Output schema:
    - set, trait_id, trait_name, trait_desc, icon
    - tier_number: The tier number (1-12)
    - tier_min: Minimum units needed for this tier
    - tier_max: Maximum units for this tier
    - tier_style: Style indicator for this tier
    - ingestion_timestamp
    
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
    
    print(f"Loaded {df.count()} traits from CSV")
    
    # Create an array of structs for all tiers (1-12)
    # Each struct contains: tier_number, tier_min, tier_max, tier_style
    tier_structs = []
    for i in range(1, 13):
        tier_structs.append(
            struct(
                lit(i).alias("tier_number"),
                col(f"tier{i}_min").alias("tier_min"),
                col(f"tier{i}_max").alias("tier_max"),
                col(f"tier{i}_style").alias("tier_style")
            )
        )
    
    # Add tiers array to dataframe
    df = df.withColumn("tiers", array(*tier_structs))
    
    # Select base columns and tiers array
    df = df.select(
        "set",
        "trait_id",
        "trait_name",
        "trait_desc",
        "icon",
        "tiers"
    )
    
    # Explode tiers array into rows
    df_exploded = df.select(
        "set",
        "trait_id",
        "trait_name",
        "trait_desc",
        "icon",
        explode("tiers").alias("tier_data")
    )
    
    # Extract tier fields from struct
    df_exploded = df_exploded.select(
        "set",
        "trait_id",
        "trait_name",
        "trait_desc",
        "icon",
        col("tier_data.tier_number").alias("tier_number"),
        col("tier_data.tier_min").alias("tier_min"),
        col("tier_data.tier_max").alias("tier_max"),
        col("tier_data.tier_style").alias("tier_style")
    )
    
    # Filter out rows where tier_min is null (indicating no tier data)
    df_exploded = df_exploded.filter(col("tier_min").isNotNull())
    
    # Add metadata columns
    df_exploded = df_exploded.withColumn("ingestion_timestamp", current_timestamp())
    
    # Show sample
    print(f"Transformed to {df_exploded.count()} trait-tier rows")
    print("Sample data (row-based tiers):")
    df_exploded.filter(col("trait_name") == df_exploded.select("trait_name").first()[0]) \
        .orderBy("tier_number") \
        .show(10, truncate=False)
    
    # Show tier distribution
    print("Tier distribution:")
    df_exploded.groupBy("tier_number").count().orderBy("tier_number").show()
    
    # Write to Delta
    df_exploded.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)
    
    print(f"✓ Traits saved to {target_path} in row-based format")
    
    return df_exploded


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
