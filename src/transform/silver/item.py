"""
Silver layer: SCD Type 2 for items (row-based attributes)
Tracks historical changes to item attributes over time
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, md5, concat_ws
from delta.tables import DeltaTable


def process_items_scd2(spark, bronze_path, silver_path):
    """
    Process items from Bronze to Silver with SCD Type 2.
    Tracks changes to item attributes over time.
    
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
    print(f"Processing items from {bronze_path} to {silver_path}")
    
    # Read bronze data
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # Create hash for change detection (includes all attributes except timestamps)
    bronze_df = bronze_df.withColumn(
        "record_hash",
        md5(concat_ws("|",
            col("set"),
            col("item_id"),
            col("item_name"),
            col("item_desc"),
            col("icon"),
            col("component1"),
            col("component2"),
            col("num_components"),
            col("unique"),
            col("attribute_name"),
            col("attribute_value")
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
        
        # Merge logic:
        # 1. Match on natural key (set, item_id, attribute_name) where is_current = true
        # 2. If hash differs, close old record and insert new one
        # 3. If hash same, do nothing
        # 4. If not matched, insert as new
        
        merge_condition = """
            silver.set = bronze.set 
            AND silver.item_id = bronze.item_id 
            AND silver.attribute_name = bronze.attribute_name
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
                "item_id": col("bronze.item_id"),
                "item_name": col("bronze.item_name"),
                "item_desc": col("bronze.item_desc"),
                "icon": col("bronze.icon"),
                "component1": col("bronze.component1"),
                "component2": col("bronze.component2"),
                "num_components": col("bronze.num_components"),
                "unique": col("bronze.unique"),
                "attribute_name": col("bronze.attribute_name"),
                "attribute_value": col("bronze.attribute_value"),
                "ingestion_timestamp": col("bronze.ingestion_timestamp"),
                "record_hash": col("bronze.record_hash"),
                "is_current": lit(True),
                "effective_start_date": current_timestamp(),
                "effective_end_date": lit(None).cast("timestamp")
            }
        ).execute()
        
        # Insert new versions for updated records
        # Find records that were just closed (updated)
        closed_records = spark.read.format("delta").load(silver_path) \
            .filter(col("is_current") == False) \
            .filter(col("effective_end_date") >= current_timestamp())
        
        if closed_records.count() > 0:
            # Get the new versions from bronze
            updated_keys = closed_records.select("set", "item_id", "attribute_name").distinct()
            
            new_versions = bronze_df.join(
                updated_keys,
                ["set", "item_id", "attribute_name"],
                "inner"
            )
            
            # Append new versions
            new_versions.write \
                .format("delta") \
                .mode("append") \
                .save(silver_path)
            
            print(f"Updated {new_versions.count()} item-attribute records")
    
    else:
        print("Silver table does not exist - creating initial load")
        
        # Initial load - all records are current
        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"Created silver table with {bronze_df.count()} item-attribute records")
    
    # Show statistics
    silver_df = spark.read.format("delta").load(silver_path)
    
    print("\n=== Silver Item Statistics ===")
    print(f"Total records: {silver_df.count()}")
    print(f"Current records: {silver_df.filter(col('is_current') == True).count()}")
    print(f"Historical records: {silver_df.filter(col('is_current') == False).count()}")
    
    print("\nSample current records:")
    silver_df.filter(col("is_current") == True) \
        .orderBy("item_name", "attribute_name") \
        .show(10, truncate=False)
    
    return silver_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver_Items_SCD2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    bronze = "data/bronze/items"
    silver = "data/silver/items"
    
    process_items_scd2(spark, bronze, silver)
    
    spark.stop()
