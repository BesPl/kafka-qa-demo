# test_producer.py
from kafka import KafkaProducer
import json
import time
import sys

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'orders.process'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode
)


def send_message(order_id, customer_id, amount, description=""):
    message = {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "timestamp": int(time.time()),
        "test_scenario": description
    }

    future = producer.send(TOPIC, key=customer_id, value=message)
    record_metadata = future.get(timeout=10)
    print(f"✅ {description:30} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")


if __name__ == "__main__":
    print("=" * 60)
    print("🧪 Kafka QA Test Producer")
    print("=" * 60)

    # Сценарий 1: Проверка партиций (один клиент → одна партиция)
    print("\n🔹 Сценарий 1: Проверка партиций (один клиент)")
    send_message("TEST-001", "QA-USER-01", 100.00, "Первое сообщение")
    send_message("TEST-002", "QA-USER-01", 150.00, "Второе сообщение")
    send_message("TEST-003", "QA-USER-01", 200.00, "Третье сообщение")

    # Сценарий 2: Разные клиенты → разные партиции
    print("\n🔹 Сценарий 2: Разные клиенты")
    send_message("TEST-004", "QA-USER-02", 50.00, "Клиент 2")
    send_message("TEST-005", "QA-USER-03", 75.00, "Клиент 3")

    # Сценарий 3: Дублирование
    print("\n🔹 Сценарий 3: Дублирование")
    send_message("TEST-006", "QA-USER-04", 300.00, "Оригинал")
    send_message("TEST-006", "QA-USER-04", 300.00, "Дубль (тот же order_id)")

    # Сценарий 4: Ошибки валидации
    print("\n🔹 Сценарий 4: Ошибки валидации")
    send_message("TEST-007", "QA-USER-05", -50.00, "Отрицательная сумма")
    # Отправка битого JSON через отдельный вызов (см. ниже)

    producer.flush()
    print("\n" + "=" * 60)
    print("✅ Все тестовые сообщения отправлены!")
    print("👉 Проверьте результаты в Kafka UI: http://localhost:8080")
    print("=" * 60)