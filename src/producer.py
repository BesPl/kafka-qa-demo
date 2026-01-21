# src/producer.py
import os
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Настройка
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TEST_TOPIC", "qa.test.topic")

print(f"🚀 Starting producer for topic: {TOPIC}")
print(f"📡 Connecting to Kafka at: {BOOTSTRAP_SERVERS}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode
)

try:
    for i in range(5):
        message = {
            "event": "test_message",
            "counter": i + 1,
            "timestamp": int(time.time()),
            "source": "producer"
        }
        key = f"user_{i % 3}"  # Распределение по 3 партициям

        print(f"📤 Sending message {i + 1}/5 with key='{key}'")
        producer.send(TOPIC, key=key, value=message)
        time.sleep(1)

    producer.flush()
    print("✅ All messages sent successfully!")

except Exception as e:
    print(f"❌ Error sending messages: {e}")
finally:
    producer.close()