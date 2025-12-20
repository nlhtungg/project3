"""
Unified Bronze Layer Job
Runs all bronze streaming transformations in parallel using a single Spark session.
This reduces overhead compared to running 3 separate Spark applications.
"""
import sys
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[2]  # .../src
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from common.io.spark_session import create_bronze_spark
from common.config.delta import get_delta_config
from common.logging.logging_setup import get_logger

# Import the transformation functions
from transform.bronze.match import create_match_stream
from transform.bronze.matchParticipant import create_match_participant_stream
from transform.bronze.participantUnit import create_participant_unit_stream

logger = get_logger("bronze-unified")


def main():
    """
    Run all bronze transformations in a single Spark session.
    Each transformation reads from a different Kafka topic and writes to a different Delta table.
    """
    logger.info("Starting unified bronze layer streaming job")
    
    # Create a single Spark session for all transformations
    spark = create_bronze_spark("bronze-unified-stream")
    cfg = get_delta_config()
    
    try:
        # Start all three streaming queries in parallel
        logger.info("Starting match stream...")
        query_match = create_match_stream(spark, cfg)
        
        logger.info("Starting match participant stream...")
        query_participant = create_match_participant_stream(spark, cfg)
        
        logger.info("Starting participant unit stream...")
        query_unit = create_participant_unit_stream(spark, cfg)
        
        logger.info("All streams started successfully")
        logger.info("Stream IDs:")
        logger.info(f"  - Match: {query_match.id}")
        logger.info(f"  - Match Participant: {query_participant.id}")
        logger.info(f"  - Participant Unit: {query_unit.id}")
        
        # Wait for all queries to finish (they run indefinitely until stopped)
        # Using awaitAnyTermination() to wait for any stream to fail
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in unified bronze job: {e}", exc_info=True)
        raise
    finally:
        logger.info("Stopping unified bronze job")
        spark.stop()


if __name__ == "__main__":
    main()
