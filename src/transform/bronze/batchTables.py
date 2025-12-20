"""
Bronze layer: Batch load all TFT tables from CSV to Delta
Main orchestration script for loading units, items, traits, and augments
"""
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_bronze_spark
from common.config.delta import get_delta_config
from common.logging.logging_setup import get_logger
from common.io.delta_io import build_paths, ensure_database, register_delta_table

from transform.bronze.unit import load_units_to_bronze
from transform.bronze.item import load_items_to_bronze
from transform.bronze.trait import load_traits_to_bronze
from transform.bronze.augment import load_augments_to_bronze

logger = get_logger("bronze-batch-tables")


def batch_load_all_tables(spark, cfg, source_dir="test_data"):
    """
    Batch load all TFT tables to Bronze Delta layer
    
    Args:
        spark: SparkSession
        cfg: DeltaLakeConfig
        source_dir: Directory containing CSV files
    """
    logger.info("="*70)
    logger.info("BRONZE LAYER - BATCH LOAD ALL TABLES")
    logger.info("="*70)
    
    bronze_bucket = cfg.get_bucket("bronze")
    
    tables_config = [
        {
            "name": "units",
            "table_name": "tft_units",
            "load_func": load_units_to_bronze,
            "source": f"{source_dir}/units.csv",
            "target": f"{bronze_bucket}/tft_units"
        },
        {
            "name": "items",
            "table_name": "tft_items",
            "load_func": load_items_to_bronze,
            "source": f"{source_dir}/items.csv",
            "target": f"{bronze_bucket}/tft_items"
        },
        {
            "name": "traits",
            "table_name": "tft_traits",
            "load_func": load_traits_to_bronze,
            "source": f"{source_dir}/traits.csv",
            "target": f"{bronze_bucket}/tft_traits"
        },
        {
            "name": "augments",
            "table_name": "tft_augments",
            "load_func": load_augments_to_bronze,
            "source": f"{source_dir}/augments.csv",
            "target": f"{bronze_bucket}/tft_augments"
        }
    ]
    
    results = {}
    
    for table in tables_config:
        logger.info(f"\n{'='*70}")
        logger.info(f"Loading table: {table['name'].upper()}")
        logger.info(f"{'='*70}")
        
        try:
            # Build paths for catalog registration
            paths = build_paths("bronze", table['table_name'], cfg)
            
            # Ensure database exists
            ensure_database(spark, paths.db_name)
            logger.info(f"Database ensured: {paths.db_name}")
            
            # Load data to Delta
            df = table['load_func'](spark, table['source'], table['target'])
            record_count = df.count()
            logger.info(f"Loaded {record_count} records to {table['target']}")
            
            # Register table with catalog
            register_delta_table(spark, paths.db_name, paths.table_name, paths.data_path)
            logger.info(f"Registered table: {paths.db_name}.{paths.table_name}")
            
            results[table['name']] = {
                "status": "SUCCESS",
                "count": record_count,
                "path": table['target'],
                "catalog": f"{paths.db_name}.{paths.table_name}"
            }
        except Exception as e:
            logger.error(f"Error loading {table['name']}: {e}", exc_info=True)
            results[table['name']] = {
                "status": "FAILED",
                "error": str(e)
            }
    
    # Print summary
    logger.info(f"\n{'='*70}")
    logger.info("LOAD SUMMARY")
    logger.info(f"{'='*70}")
    
    for table_name, result in results.items():
        status_symbol = "✓" if result['status'] == "SUCCESS" else "✗"
        logger.info(f"\n{status_symbol} {table_name.upper()}: {result['status']}")
        if result['status'] == "SUCCESS":
            logger.info(f"   Records: {result['count']}")
            logger.info(f"   Path: {result['path']}")
            logger.info(f"   Catalog: {result.get('catalog', 'N/A')}")
        else:
            logger.error(f"   Error: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n{'='*70}")
    
    return results


def main():
    """Main entry point"""
    logger.info("Starting bronze batch load job")
    
    # Create Spark session and config
    spark = create_bronze_spark("bronze-batch-tft-tables")
    cfg = get_delta_config()
    
    try:
        # Run batch load
        results = batch_load_all_tables(spark, cfg)
        
        # Check if all succeeded
        all_success = all(r['status'] == 'SUCCESS' for r in results.values())
        
        if all_success:
            logger.info("✓ All tables loaded successfully!")
            exit_code = 0
        else:
            logger.warning("✗ Some tables failed to load")
            exit_code = 1
        
    except Exception as e:
        logger.error(f"Batch load failed: {e}", exc_info=True)
        exit_code = 1
    
    finally:
        logger.info("Stopping bronze batch job")
        spark.stop()
    
    return exit_code


if __name__ == "__main__":
    exit(main())
