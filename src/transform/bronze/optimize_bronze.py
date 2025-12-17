"""
Bronze Layer Storage Optimization Job

This job optimizes Delta Lake storage for bronze layer tables by:
1. Compacting small files using OPTIMIZE
2. Z-ORDERING data for better query performance
3. Optionally running VACUUM to clean up old files (with safe retention)

Safe to run - does not destroy any data.
"""
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_bronze_spark
from common.io.delta_io import build_paths
from common.config.delta import get_delta_config
from common.logging.logging_setup import get_logger
from delta.tables import DeltaTable

logger = get_logger("bronze-optimize")

# Configuration
# IMPORTANT for streaming: do NOT use very small retention
# windows or aggressive VACUUM while structured streaming jobs
# are still reading older versions of the table.
VACUUM_RETENTION_HOURS = 168  # 7 days retention for VACUUM (Delta default)
RUN_VACUUM = False  # Disable VACUUM by default to avoid deleting files needed by streams

# Bronze tables to optimize
BRONZE_TABLES = [
    {
        "name": "tft_matches",
        "zorder_columns": ["match_id", "gameId"]  # Columns for Z-ORDER optimization
    },
    {
        "name": "tft_match_participants",
        "zorder_columns": ["match_id", "gameId", "puuid"]
    },
    {
        "name": "tft_participant_units",
        "zorder_columns": ["match_id", "gameId", "puuid"]
    }
]


def get_table_stats(spark: SparkSession, table_path: str) -> dict:
    """Get storage statistics for a Delta table."""
    try:
        # Read table details
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get file count and size
        files_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        stats = files_df.collect()[0]
        
        return {
            "num_files": stats["numFiles"],
            "size_in_bytes": stats["sizeInBytes"],
            "size_in_mb": round(stats["sizeInBytes"] / (1024 * 1024), 2),
            "size_in_gb": round(stats["sizeInBytes"] / (1024 * 1024 * 1024), 2)
        }
    except Exception as e:
        logger.warning(f"Could not get stats for {table_path}: {e}")
        return {
            "num_files": 0,
            "size_in_bytes": 0,
            "size_in_mb": 0,
            "size_in_gb": 0
        }


def optimize_table(spark: SparkSession, table_name: str, table_path: str, zorder_columns: list = None):
    """
    Optimize a Delta table by compacting small files and applying Z-ORDER.
    
    Args:
        spark: SparkSession
        table_name: Name of the table
        table_path: Path to the Delta table
        zorder_columns: Columns to use for Z-ORDER optimization
    """
    logger.info(f"=" * 80)
    logger.info(f"Optimizing table: {table_name}")
    logger.info(f"Path: {table_path}")
    
    try:
        # Get stats before optimization
        logger.info("Getting statistics before optimization...")
        stats_before = get_table_stats(spark, table_path)
        logger.info(f"Before optimization:")
        logger.info(f"  - Number of files: {stats_before['num_files']}")
        logger.info(f"  - Total size: {stats_before['size_in_gb']} GB ({stats_before['size_in_mb']} MB)")
        
        # Run OPTIMIZE
        logger.info("Running OPTIMIZE to compact small files...")
        delta_table = DeltaTable.forPath(spark, table_path)
        
        if zorder_columns:
            logger.info(f"Applying Z-ORDER on columns: {zorder_columns}")
            # OPTIMIZE with Z-ORDER
            optimize_result = delta_table.optimize().executeZOrderBy(zorder_columns)
        else:
            # OPTIMIZE without Z-ORDER
            optimize_result = delta_table.optimize().executeCompaction()
        
        # Log optimization results
        result_df = optimize_result
        for row in result_df.collect():
            logger.info(f"Optimization metrics:")
            logger.info(f"  - Files added: {row['metrics']['numFilesAdded']}")
            logger.info(f"  - Files removed: {row['metrics']['numFilesRemoved']}")
            if 'numBatches' in row['metrics']:
                logger.info(f"  - Batches processed: {row['metrics']['numBatches']}")
        
        # Get stats after optimization
        logger.info("Getting statistics after optimization...")
        stats_after = get_table_stats(spark, table_path)
        logger.info(f"After optimization:")
        logger.info(f"  - Number of files: {stats_after['num_files']}")
        logger.info(f"  - Total size: {stats_after['size_in_gb']} GB ({stats_after['size_in_mb']} MB)")
        
        # Calculate improvements
        files_reduced = stats_before['num_files'] - stats_after['num_files']
        files_reduction_pct = (files_reduced / stats_before['num_files'] * 100) if stats_before['num_files'] > 0 else 0
        
        logger.info(f"Improvements:")
        logger.info(f"  - Files reduced: {files_reduced} ({files_reduction_pct:.2f}%)")
        
        if stats_before['size_in_bytes'] > stats_after['size_in_bytes']:
            size_saved_mb = stats_before['size_in_mb'] - stats_after['size_in_mb']
            logger.info(f"  - Storage saved: {size_saved_mb:.2f} MB")
        
        logger.info(f"✓ Table {table_name} optimized successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error optimizing table {table_name}: {e}", exc_info=True)
        return False


def vacuum_table(spark: SparkSession, table_name: str, table_path: str, retention_hours: int):
    """
    Run VACUUM on a Delta table to remove old files.
    
    WARNING: This permanently deletes files. Only run after testing OPTIMIZE.
    
    Args:
        spark: SparkSession
        table_name: Name of the table
        table_path: Path to the Delta table
        retention_hours: Retention period in hours
    """
    logger.info(f"Running VACUUM on {table_name} (retention: {retention_hours} hours)...")
    
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Run VACUUM
        vacuum_result = delta_table.vacuum(retention_hours)
        
        logger.info(f"✓ VACUUM completed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error running VACUUM on {table_name}: {e}", exc_info=True)
        return False


def optimize_all_bronze_tables(spark: SparkSession, cfg, run_vacuum: bool = False):
    """
    Optimize all bronze layer tables.
    
    Args:
        spark: SparkSession
        cfg: Delta configuration
        run_vacuum: Whether to run VACUUM after optimization
    """
    logger.info("=" * 80)
    logger.info("BRONZE LAYER STORAGE OPTIMIZATION")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Tables to process: {len(BRONZE_TABLES)}")
    logger.info(f"Run VACUUM: {run_vacuum}")
    if run_vacuum:
        logger.info(f"VACUUM retention: {VACUUM_RETENTION_HOURS} hours")
    logger.info("=" * 80)
    
    successful = 0
    failed = 0
    
    # Process each table
    for table_config in BRONZE_TABLES:
        table_name = table_config["name"]
        zorder_columns = table_config.get("zorder_columns", [])
        
        # Build table path
        paths = build_paths("bronze", table_name, cfg)
        
        # Check if table exists
        try:
            DeltaTable.forPath(spark, paths.data_path)
        except Exception as e:
            logger.warning(f"Table {table_name} not found at {paths.data_path}, skipping...")
            continue
        
        # Optimize the table
        if optimize_table(spark, table_name, paths.data_path, zorder_columns):
            successful += 1
            
            # Run VACUUM if enabled
            if run_vacuum:
                vacuum_table(spark, table_name, paths.data_path, VACUUM_RETENTION_HOURS)
        else:
            failed += 1
    
    # Summary
    logger.info("=" * 80)
    logger.info("OPTIMIZATION SUMMARY")
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total tables: {len(BRONZE_TABLES)}")
    logger.info(f"Successfully optimized: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info("=" * 80)
    
    return successful, failed


def main():
    """Main function to run bronze layer optimization."""
    logger.info("Starting Bronze Layer Storage Optimization Job")
    
    # Create Spark session
    spark = create_bronze_spark("bronze-optimize")
    
    # Enable Delta Lake optimizations
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    
    cfg = get_delta_config()
    
    try:
        # Run optimization
        successful, failed = optimize_all_bronze_tables(spark, cfg, run_vacuum=RUN_VACUUM)
        
        if failed == 0:
            logger.info("All tables optimized successfully!")
        else:
            logger.warning(f"{failed} table(s) failed to optimize")
        
    except Exception as e:
        logger.error(f"Error in optimization job: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Bronze optimization job completed")


if __name__ == "__main__":
    main()
