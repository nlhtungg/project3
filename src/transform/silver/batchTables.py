"""
Silver layer: Batch process all TFT tables with SCD Type 2
Main orchestration script for processing units, items, traits, and augments
"""
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_silver_spark
from common.config.delta import get_delta_config
from common.logging.logging_setup import get_logger
from common.io.delta_io import build_paths, ensure_database, register_delta_table

from transform.silver.unit import process_units_scd2
from transform.silver.item import process_items_scd2
from transform.silver.trait import process_traits_scd2
from transform.silver.augment import process_augments_scd2

logger = get_logger("silver-batch-tables")


def batch_process_all_tables(spark, cfg):
    """
    Batch process all TFT tables from Bronze to Silver with SCD Type 2
    
    Args:
        spark: SparkSession
        cfg: DeltaLakeConfig
    """
    logger.info("="*70)
    logger.info("SILVER LAYER - BATCH PROCESS ALL TABLES (SCD2)")
    logger.info("="*70)
    
    bronze_bucket = cfg.get_bucket("bronze")
    silver_bucket = cfg.get_bucket("silver")
    
    tables_config = [
        {
            "name": "units",
            "table_name": "tft_units",
            "process_func": process_units_scd2,
            "bronze_path": f"{bronze_bucket}/tft_units",
            "silver_path": f"{silver_bucket}/tft_units"
        },
        {
            "name": "items",
            "table_name": "tft_items",
            "process_func": process_items_scd2,
            "bronze_path": f"{bronze_bucket}/tft_items",
            "silver_path": f"{silver_bucket}/tft_items"
        },
        {
            "name": "traits",
            "table_name": "tft_traits",
            "process_func": process_traits_scd2,
            "bronze_path": f"{bronze_bucket}/tft_traits",
            "silver_path": f"{silver_bucket}/tft_traits"
        },
        {
            "name": "augments",
            "table_name": "tft_augments",
            "process_func": process_augments_scd2,
            "bronze_path": f"{bronze_bucket}/tft_augments",
            "silver_path": f"{silver_bucket}/tft_augments"
        }
    ]
    
    results = {}
    
    for table in tables_config:
        logger.info(f"\n{'='*70}")
        logger.info(f"Processing table: {table['name'].upper()}")
        logger.info(f"{'='*70}")
        
        try:
            # Build paths for catalog registration
            paths = build_paths("silver", table['table_name'], cfg)
            
            # Ensure database exists
            ensure_database(spark, paths.db_name)
            logger.info(f"Database ensured: {paths.db_name}")
            
            # Process data from Bronze to Silver with SCD2
            df = table['process_func'](spark, table['bronze_path'], table['silver_path'])
            
            total_count = df.count()
            current_count = df.filter("is_current = true").count()
            historical_count = df.filter("is_current = false").count()
            
            logger.info(f"Processed {total_count} total records ({current_count} current, {historical_count} historical)")
            
            # Register table with catalog
            register_delta_table(spark, paths.db_name, paths.table_name, paths.data_path)
            logger.info(f"Registered table: {paths.db_name}.{paths.table_name}")
            
            results[table['name']] = {
                "status": "SUCCESS",
                "total_count": total_count,
                "current_count": current_count,
                "historical_count": historical_count,
                "path": table['silver_path'],
                "catalog": f"{paths.db_name}.{paths.table_name}"
            }
        except Exception as e:
            logger.error(f"Error processing {table['name']}: {e}", exc_info=True)
            results[table['name']] = {
                "status": "FAILED",
                "error": str(e)
            }
    
    # Print summary
    logger.info(f"\n{'='*70}")
    logger.info("PROCESS SUMMARY")
    logger.info(f"{'='*70}")
    
    for table_name, result in results.items():
        status_symbol = "✓" if result['status'] == "SUCCESS" else "✗"
        logger.info(f"\n{status_symbol} {table_name.upper()}: {result['status']}")
        if result['status'] == "SUCCESS":
            logger.info(f"   Total Records: {result['total_count']}")
            logger.info(f"   Current Records: {result['current_count']}")
            logger.info(f"   Historical Records: {result['historical_count']}")
            logger.info(f"   Path: {result['path']}")
            logger.info(f"   Catalog: {result.get('catalog', 'N/A')}")
        else:
            logger.error(f"   Error: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n{'='*70}")
    
    return results


def main():
    """Main entry point"""
    logger.info("Starting silver batch process job")
    
    # Create Spark session and config
    spark = create_silver_spark("silver-batch-tft-tables")
    cfg = get_delta_config()
    
    try:
        # Run batch process
        results = batch_process_all_tables(spark, cfg)
        
        # Check if all succeeded
        all_success = all(r['status'] == 'SUCCESS' for r in results.values())
        
        if all_success:
            logger.info("✓ All tables processed successfully!")
            exit_code = 0
        else:
            logger.warning("✗ Some tables failed to process")
            exit_code = 1
        
    except Exception as e:
        logger.error(f"Batch process failed: {e}", exc_info=True)
        exit_code = 1
    
    finally:
        logger.info("Stopping silver batch job")
        spark.stop()
    
    return exit_code


if __name__ == "__main__":
    exit(main())
