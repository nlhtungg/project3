"""
Silver layer: SCD Type 2 for units
Tracks historical changes to unit stats and abilities over time
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, md5, concat_ws
from delta.tables import DeltaTable


def process_units_scd2(spark, bronze_path, silver_path):
    """
    Process units from Bronze to Silver with SCD Type 2.
    Tracks changes to unit stats, traits, and abilities over time.
    
    SCD2 columns added:
    - is_current: Boolean indicating if this is the current version
    - effective_start_date: When this version became effective
    - effective_end_date: When this version was superseded (null for current)
    - record_hash: MD5 hash of key attributes for change detection
    
    Args:
        spark: SparkSession
        bronze_path: Path to Bronze Delta table
        silver_path: Path to Silver Delta table
    """
    print(f"Processing units from {bronze_path} to {silver_path}")
    
    # Read bronze data
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # Create hash for change detection - includes all attributes
    bronze_df = bronze_df.withColumn(
        "record_hash",
        md5(concat_ws("|",
            col("set"),
            col("unit_id"),
            col("unit_name"),
            col("cost"),
            col("trait1"),
            col("trait2"),
            col("trait3"),
            col("trait4"),
            col("num_traits"),
            col("all_traits"),
            col("ability_name"),
            col("ability_desc"),
            col("health"),
            col("armor"),
            col("magic_resist"),
            col("attack_damage"),
            col("attack_speed"),
            col("attack_range"),
            col("mana_start"),
            col("mana_max"),
            col("crit_chance"),
            col("crit_multiplier")
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
            AND silver.unit_id = bronze.unit_id
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
                "unit_id": col("bronze.unit_id"),
                "unit_name": col("bronze.unit_name"),
                "cost": col("bronze.cost"),
                "trait1": col("bronze.trait1"),
                "trait2": col("bronze.trait2"),
                "trait3": col("bronze.trait3"),
                "trait4": col("bronze.trait4"),
                "num_traits": col("bronze.num_traits"),
                "all_traits": col("bronze.all_traits"),
                "ability_name": col("bronze.ability_name"),
                "ability_desc": col("bronze.ability_desc"),
                "health": col("bronze.health"),
                "armor": col("bronze.armor"),
                "magic_resist": col("bronze.magic_resist"),
                "attack_damage": col("bronze.attack_damage"),
                "attack_speed": col("bronze.attack_speed"),
                "attack_range": col("bronze.attack_range"),
                "mana_start": col("bronze.mana_start"),
                "mana_max": col("bronze.mana_max"),
                "crit_chance": col("bronze.crit_chance"),
                "crit_multiplier": col("bronze.crit_multiplier"),
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
            updated_keys = closed_records.select("set", "unit_id").distinct()
            
            new_versions = bronze_df.join(
                updated_keys,
                ["set", "unit_id"],
                "inner"
            )
            
            new_versions.write \
                .format("delta") \
                .mode("append") \
                .save(silver_path)
            
            print(f"Updated {new_versions.count()} unit records")
    
    else:
        print("Silver table does not exist - creating initial load")
        
        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"Created silver table with {bronze_df.count()} unit records")
    
    # Show statistics
    silver_df = spark.read.format("delta").load(silver_path)
    
    print("\n=== Silver Unit Statistics ===")
    print(f"Total records: {silver_df.count()}")
    print(f"Current records: {silver_df.filter(col('is_current') == True).count()}")
    print(f"Historical records: {silver_df.filter(col('is_current') == False).count()}")
    
    print("\nSample current records:")
    silver_df.filter(col("is_current") == True) \
        .select("unit_name", "cost", "health", "attack_damage", "all_traits", "is_current") \
        .orderBy("cost", "unit_name") \
        .show(10, truncate=False)
    
    print("\nUnits by cost:")
    silver_df.filter(col("is_current") == True) \
        .groupBy("cost").count().orderBy("cost") \
        .show()
    
    return silver_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver_Units_SCD2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    bronze = "data/bronze/units"
    silver = "data/silver/units"
    
    process_units_scd2(spark, bronze, silver)
    
    spark.stop()
