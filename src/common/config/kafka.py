# src/common/config/kafka.py
from dataclasses import dataclass
import os


@dataclass
class KafkaConfig:
    """Kafka config dùng cho Spark Structured Streaming."""

    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092"
    )
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    group_id_prefix: str = os.getenv("KAFKA_GROUP_ID_PREFIX", "lakehouse")

    # Có thể mở rộng thêm SASL/SSL sau
    sasl_mechanism: str | None = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username: str | None = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: str | None = os.getenv("KAFKA_SASL_PASSWORD")

    def group_id_for(self, job_name: str) -> str:
        return f"{self.group_id_prefix}-{job_name}"

    def spark_options(self) -> dict[str, str]:
        """
        Trả về các option chung cho .readStream.format("kafka").
        """
        opts: dict[str, str] = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "kafka.security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            opts["kafka.sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username and self.sasl_password:
            opts["kafka.sasl.jaas.config"] = (
                "org.apache.kafka.common.security.plain.PlainLoginModule "
                f"required username='{self.sasl_username}' "
                f"password='{self.sasl_password}';"
            )
        return opts


def get_kafka_config() -> KafkaConfig:
    return KafkaConfig()
