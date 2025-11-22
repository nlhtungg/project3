      
import random
import time
import json
from kafka import KafkaProducer

# List of topics to send messages to
topics = ['topic-a', 'topic-b', 'topic-c']

# Kafka broker addresses
bootstrap_servers = ['localhost:9092', 'localhost:9192']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_random_message():
    return {
        "timestamp": time.time(),
        "value": random.randint(1, 100),
        "status": random.choice(["ok", "warning", "error"]),
        "source": random.choice(["sensor-1", "sensor-2", "sensor-3"])
    }

try:
    while True:
        topic = random.choice(topics)
        message = generate_random_message()
        producer.send(topic, value=message)
        print(f"Sent to {topic}: {message}")
        time.sleep(random.uniform(1, 3))  # Sleep 1–3 seconds
except KeyboardInterrupt:
    print("Stopped sending messages.")
finally:
    producer.flush()
    producer.close()

    