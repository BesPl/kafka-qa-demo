# src/consumer.py
import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TEST_TOPIC", "qa.test.topic")
GROUP_ID = os.getenv("CONSUMER_GROUP", "qa-test-group")

print(f"👂 Starting consumer for topic: {TOPIC}")
print(f"📡 Connecting to Kafka at: {BOOTSTRAP_SERVERS}")
print(f"👥 Consumer group: {GROUP_ID}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    print("⏳ Waiting for messages... (Ctrl+C to stop)")
    count = 0
    for message in consumer:
        count += 1
        print(f"📥 Received message #{count}:")
        print(f"   Partition: {message.partition}")
        print(f"   Offset: {message.offset}")
        print(f"   Key: {message.key.decode() if message.key else 'None'}")
        print(f"   Value: {message.value}")
        print("-" * 50)

        # Имитация обработки
        consumer.commit()

        # Остановка после 5 сообщений
        if count >= 5:
            break

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user")
except Exception as e:
    print(f"❌ Error consuming messages: {e}")
finally:
    consumer.close()
    print("👋 Consumer closed")