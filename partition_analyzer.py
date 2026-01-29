# partition_analyzer.py
from kafka import KafkaConsumer
import json
from collections import defaultdict

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'orders.process'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id='partition-analyzer',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("=" * 60)
print("📊 Анализ партиций для топика:", TOPIC)
print("=" * 60)

partition_stats = defaultdict(list)

for message in consumer:
    partition_stats[message.partition].append({
        'offset': message.offset,
        'key': message.key.decode() if message.key else 'None',
        'order_id': message.value.get('order_id', 'N/A')
    })

    # Остановка после 20 сообщений для анализа
    if sum(len(msgs) for msgs in partition_stats.values()) >= 20:
        break

consumer.close()

# Вывод статистики
for partition, messages in sorted(partition_stats.items()):
    print(f"\n📦 Партиция {partition}: {len(messages)} сообщений")
    print("   Последовательность offset:", [m['offset'] for m in messages])

    # Проверка порядка
    offsets = [m['offset'] for m in messages]
    if offsets == sorted(offsets):
        print("   ✅ Порядок соблюдён")
    else:
        print("   ❌ Порядок НАРУШЕН!")

    # Проверка ключей
    keys = [m['key'] for m in messages]
    unique_keys = set(keys)
    print(f"   Ключи: {', '.join(unique_keys)}")

print("\n" + "=" * 60)
print("✅ Анализ завершён")
print("=" * 60)