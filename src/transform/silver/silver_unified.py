"""
Unified Silver Layer Job
Runs all silver layer SCD2 transformations using a single Spark session.
Streams are started sequentially with limitations to handle large bronze tables.
This reduces overhead compared to running 3 separate Spark applications.
"""
import sys
from pathlib import Path
import time

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_silver_spark
from common.config.delta import get_delta_config
from common.logging.logging_setup import get_logger

# Import the transformation functions
from transform.silver.match import create_match_silver_stream
from transform.silver.matchParticipant import create_match_participant_silver_stream
from transform.silver.participantUnit import create_participant_unit_silver_stream

logger = get_logger("silver-unified")

# Configuration for handling large bronze tables
STARTUP_DELAY_SECONDS = 10  # Delay between starting each stream
MAX_FILES_PER_TRIGGER = 50  # Limit files processed per trigger to prevent overload


def main():
    """
    Run all silver layer SCD2 transformations in a single Spark session.
    Streams are started sequentially with delays to handle large bronze tables.
    Each transformation reads from a different bronze Delta table and writes to a different silver Delta table.
    """
    logger.info("Starting unified silver layer streaming job")
    logger.info(f"Configuration: startup_delay={STARTUP_DELAY_SECONDS}s, max_files_per_trigger={MAX_FILES_PER_TRIGGER}")
    
    # Create a single Spark session for all transformations
    spark = create_silver_spark("silver-unified-stream")
    cfg = get_delta_config()
    
    try:
        # Start streams sequentially with delays to prevent resource contention
        logger.info("Starting match silver stream...")
        query_match = create_match_silver_stream(spark, cfg, max_files_per_trigger=MAX_FILES_PER_TRIGGER)
        logger.info(f"Match stream started with ID: {query_match.id}")
        logger.info(f"Waiting {STARTUP_DELAY_SECONDS} seconds before starting next stream...")
        time.sleep(STARTUP_DELAY_SECONDS)
        
        logger.info("Starting match participant silver stream...")
        query_participant = create_match_participant_silver_stream(spark, cfg, max_files_per_trigger=MAX_FILES_PER_TRIGGER)
        logger.info(f"Match participant stream started with ID: {query_participant.id}")
        logger.info(f"Waiting {STARTUP_DELAY_SECONDS} seconds before starting next stream...")
        time.sleep(STARTUP_DELAY_SECONDS)
        
        logger.info("Starting participant unit silver stream...")
        query_unit = create_participant_unit_silver_stream(spark, cfg, max_files_per_trigger=MAX_FILES_PER_TRIGGER)
        logger.info(f"Participant unit stream started with ID: {query_unit.id}")
        
        logger.info("All silver streams started successfully")
        logger.info("Active streams:")
        logger.info(f"  - Match SCD2: {query_match.id}")
        logger.info(f"  - Match Participant SCD2: {query_participant.id}")
        logger.info(f"  - Participant Unit SCD2: {query_unit.id}")
        
        # Monitor active streams
        logger.info("Monitoring streams...")
        active_streams = spark.streams.active
        logger.info(f"Total active streams: {len(active_streams)}")
        
        # Wait for all queries to finish (they run indefinitely until stopped)
        # Using awaitAnyTermination() to wait for any stream to fail
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in unified silver job: {e}", exc_info=True)
        raise
    finally:
        logger.info("Stopping unified silver job")
        spark.stop()


if __name__ == "__main__":
    main()
