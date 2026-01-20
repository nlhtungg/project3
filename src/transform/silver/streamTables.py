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

# Configuration for lightweight append-only processing
STARTUP_DELAY_SECONDS = 5  # Delay between starting each stream
MAX_BYTES_PER_TRIGGER = "50m"  # Larger batches now that merge is removed - 50MB per micro-batch
RUN_CONCURRENTLY = True  # Run all 3 streams together


def main():
    """
    Run all silver layer transformations in a single Spark session.
    Uses append-only writes with batch deduplication for fast processing.
    Each transformation reads from a different bronze Delta table and writes to a different silver Delta table.
    """
    logger.info("Starting unified silver layer streaming job - APPEND MODE")
    logger.info(f"Configuration: max_bytes_per_trigger={MAX_BYTES_PER_TRIGGER}, concurrent={RUN_CONCURRENTLY}")
    
    # Create a single Spark session for all transformations
    spark = create_silver_spark("silver-unified-stream")
    cfg = get_delta_config()
    
    try:
        if RUN_CONCURRENTLY:
            logger.info("Running all streams concurrently (high memory usage)...")
            # Start all streams together
            query_match = create_match_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            time.sleep(STARTUP_DELAY_SECONDS)
            query_participant = create_match_participant_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            time.sleep(STARTUP_DELAY_SECONDS)
            query_unit = create_participant_unit_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            
            logger.info("All streams started successfully")
            logger.info("Stream IDs:")
            logger.info(f"  - Match: {query_match.id}")
            logger.info(f"  - Match Participant: {query_participant.id}")
            logger.info(f"  - Participant Unit: {query_unit.id}")
            
            # Wait for all queries to finish (they run indefinitely until stopped)
            logger.info("Waiting for streams to terminate (Ctrl+C to stop)...")
            query_match.awaitTermination()
            query_participant.awaitTermination()
            query_unit.awaitTermination()
        else:
            logger.info("Running streams ONE AT A TIME (memory efficient)...")
            
            # Process match stream continuously
            logger.info("=== Processing Match stream ===")
            query_match = create_match_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            logger.info(f"Match stream ID: {query_match.id}. Running continuously...")
            query_match.awaitTermination()
            logger.info("Match stream terminated")
            
            # Process match participant stream continuously
            logger.info("=== Processing Match Participant stream ===")
            query_participant = create_match_participant_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            logger.info(f"Match participant stream ID: {query_participant.id}. Running continuously...")
            query_participant.awaitTermination()
            logger.info("Match participant stream terminated")
            
            # Process participant unit stream continuously
            logger.info("=== Processing Participant Unit stream ===")
            query_unit = create_participant_unit_silver_stream(spark, cfg, max_bytes_per_trigger=MAX_BYTES_PER_TRIGGER)
            logger.info(f"Participant unit stream ID: {query_unit.id}. Running continuously...")
            query_unit.awaitTermination()
            logger.info("Participant unit stream terminated")
        
    except Exception as e:
        logger.error(f"Error in unified silver job: {e}", exc_info=True)
        raise
    finally:
        logger.info("Stopping unified silver job")
        spark.stop()


if __name__ == "__main__":
    main()
