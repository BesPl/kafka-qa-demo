# dlq_checker.py
from kafka import KafkaConsumer
import json

BOOTSTRAP_SERVERS = 'localhost:9092'
DLQ_TOPIC = 'orders.process.dlq'

consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id='dlq-checker',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("=" * 60)
print("🔍 Проверка Dead Letter Queue:", DLQ_TOPIC)
print("=" * 60)

errors_found = 0
for message in consumer:
    errors_found += 1
    error_msg = message.value.get('error', 'N/A')
    original = message.value.get('original_message', {})
    order_id = original.get('order_id', 'N/A')

    print(f"\n❌ Ошибка #{errors_found}:")
    print(f"   Order ID: {order_id}")
    print(f"   Причина: {error_msg}")
    print(f"   Сообщение: {original}")

consumer.close()

print("\n" + "=" * 60)
if errors_found > 0:
    print(f"✅ Найдено ошибок: {errors_found}")
    print("   Все ошибочные сообщения корректно попали в DLQ")
else:
    print("⚠️  DLQ пуст — либо ошибок нет, либо система не ловит ошибки")
print("=" * 60)