"""
Silver layer: SCD Type 2 for traits (row-based tiers)
Tracks historical changes to trait tiers over time
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, md5, concat_ws
from delta.tables import DeltaTable


def process_traits_scd2(spark, bronze_path, silver_path):
    """
    Process traits from Bronze to Silver with SCD Type 2.
    Tracks changes to trait tiers over time.
    
    SCD2 columns added:
    - is_current: Boolean indicating if this is the current version
    - effective_start_date: When this version became effective
    - effective_end_date: When this version was superseded (null for current)
    - record_hash: MD5 hash of key attributes for change detection
    
    Args:
        spark: SparkSession
        bronze_path: Path to Bronze Delta table (row-based)
        silver_path: Path to Silver Delta table
    """
    print(f"Processing traits from {bronze_path} to {silver_path}")
    
    # Read bronze data
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # Create hash for change detection
    bronze_df = bronze_df.withColumn(
        "record_hash",
        md5(concat_ws("|",
            col("set"),
            col("trait_id"),
            col("trait_name"),
            col("trait_desc"),
            col("icon"),
            col("tier_number"),
            col("tier_min"),
            col("tier_max"),
            col("tier_style")
        ))
    )
    
    # Add SCD2 columns for new records
    bronze_df = bronze_df \
        .withColumn("is_current", lit(True)) \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
    
    # Check if silver table exists
    if DeltaTable.isDeltaTable(spark, silver_path):
        print("Silver table exists - performing SCD2 merge")
        
        silver_table = DeltaTable.forPath(spark, silver_path)
        
        merge_condition = """
            silver.set = bronze.set 
            AND silver.trait_id = bronze.trait_id 
            AND silver.tier_number = bronze.tier_number
            AND silver.is_current = true
        """
        
        silver_table.alias("silver").merge(
            bronze_df.alias("bronze"),
            merge_condition
        ).whenMatchedUpdate(
            condition = "silver.record_hash != bronze.record_hash",
            set = {
                "is_current": lit(False),
                "effective_end_date": current_timestamp()
            }
        ).whenNotMatchedInsert(
            values = {
                "set": col("bronze.set"),
                "trait_id": col("bronze.trait_id"),
                "trait_name": col("bronze.trait_name"),
                "trait_desc": col("bronze.trait_desc"),
                "icon": col("bronze.icon"),
                "tier_number": col("bronze.tier_number"),
                "tier_min": col("bronze.tier_min"),
                "tier_max": col("bronze.tier_max"),
                "tier_style": col("bronze.tier_style"),
                "ingestion_timestamp": col("bronze.ingestion_timestamp"),
                "record_hash": col("bronze.record_hash"),
                "is_current": lit(True),
                "effective_start_date": current_timestamp(),
                "effective_end_date": lit(None).cast("timestamp")
            }
        ).execute()
        
        # Insert new versions for updated records
        closed_records = spark.read.format("delta").load(silver_path) \
            .filter(col("is_current") == False) \
            .filter(col("effective_end_date") >= current_timestamp())
        
        if closed_records.count() > 0:
            updated_keys = closed_records.select("set", "trait_id", "tier_number").distinct()
            
            new_versions = bronze_df.join(
                updated_keys,
                ["set", "trait_id", "tier_number"],
                "inner"
            )
            
            new_versions.write \
                .format("delta") \
                .mode("append") \
                .save(silver_path)
            
            print(f"Updated {new_versions.count()} trait-tier records")
    
    else:
        print("Silver table does not exist - creating initial load")
        
        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"Created silver table with {bronze_df.count()} trait-tier records")
    
    # Show statistics
    silver_df = spark.read.format("delta").load(silver_path)
    
    print("\n=== Silver Trait Statistics ===")
    print(f"Total records: {silver_df.count()}")
    print(f"Current records: {silver_df.filter(col('is_current') == True).count()}")
    print(f"Historical records: {silver_df.filter(col('is_current') == False).count()}")
    
    print("\nSample current records:")
    silver_df.filter(col("is_current") == True) \
        .orderBy("trait_name", "tier_number") \
        .show(10, truncate=False)
    
    return silver_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver_Traits_SCD2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    bronze = "data/bronze/traits"
    silver = "data/silver/traits"
    
    process_traits_scd2(spark, bronze, silver)
    
    spark.stop()
