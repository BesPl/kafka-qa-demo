# test_producer.py
from kafka import KafkaProducer
import json
import time
from os import getenv

# 🔑 Только kafka:9092 — НЕ host.docker.internal!
BOOTSTRAP_SERVERS = getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = 'orders.process'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode,
    # Добавим таймауты для отладки
    reconnect_backoff_ms=1000,
    max_block_ms=30000
)

def send_message(order_id, customer_id, amount, description=""):
    message = {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "timestamp": int(time.time()),
        "test_scenario": description
    }
    try:
        future = producer.send(TOPIC, key=customer_id, value=message)
        record_metadata = future.get(timeout=10)
        print(f"✅ {description:<30} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
    except Exception as e:
        print(f"❌ Ошибка при отправке {order_id}: {type(e).__name__}: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 Kafka QA Test Producer")
    print("=" * 60)

    for i, (oid, cid, amt, desc) in enumerate([
        ("TEST-001", "QA-USER-01", 100.00, "Первое сообщение"),
        ("TEST-002", "QA-USER-01", 150.00, "Второе сообщение"),
        ("TEST-003", "QA-USER-01", 200.00, "Третье сообщение"),
        ("TEST-004", "QA-USER-02", 50.00, "Клиент 2"),
        ("TEST-005", "QA-USER-03", 75.00, "Клиент 3"),
        ("TEST-006", "QA-USER-04", 300.00, "Оригинал"),
        ("TEST-006", "QA-USER-04", 300.00, "Дубль"),
        ("TEST-007", "QA-USER-05", -50.00, "Отрицательная сумма"),
    ]):
        send_message(oid, cid, amt, desc)

    producer.flush()
    print("\n" + "=" * 60)
    print("✅ Все тестовые сообщения отправлены!")
    print("👉 Проверьте в Kafka UI: http://localhost:8080")
    print("=" * 60)