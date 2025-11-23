# src/common/io/streaming_utils.py
import signal
from typing import Optional, Callable

from pyspark.sql.streaming import StreamingQuery
from common.logging.logging_setup import get_logger

logger = get_logger("streaming")


class GracefulKiller:
    """
    Dùng để bắt SIGINT/SIGTERM và stop nhiều StreamingQuery một cách êm.
    Lấy ý tưởng từ SummonerDataConsumer. :contentReference[oaicite:8]{index=8}
    """

    def __init__(self):
        self.queries: list[StreamingQuery] = []
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def register(self, query: StreamingQuery):
        self.queries.append(query)

    def _handler(self, signum, frame):
        logger.info(f"Received signal {signum}, stopping streaming queries...")
        for q in self.queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as e:
                logger.warning(f"Error stopping query {q.id}: {e}")


def wait_for_termination(
    *queries: StreamingQuery,
    on_error: Optional[Callable[[Exception], None]] = None,
):
    """
    Chờ nhiều query kết thúc, dùng cho multi-sink
    (ví dụ: Delta + console như test_read_stream.py). :contentReference[oaicite:9]{index=9}
    """
    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, stopping queries...")
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as e:
                logger.warning(f"Error stopping query {q.id}: {e}")
    except Exception as e:
        logger.error(f"Fatal error in streaming: {e}")
        if on_error:
            on_error(e)
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception:
                pass
