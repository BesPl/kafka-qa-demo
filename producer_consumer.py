# producer_consumer.py
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import uuid
from loguru import logger
import os

# Чтение конфигурации
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
MAIN_TOPIC = os.getenv('MAIN_TOPIC', 'orders.process')
DLQ_TOPIC = f"{MAIN_TOPIC}.dlq"
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'order-processing-group')
MODE = os.getenv('MODE', 'producer')  # producer или consumer

logger.add("logs/{time}.log", rotation="1 MB", retention="10 days")

# ==================== PRODUCER ====================
if MODE == 'producer':
    logger.info("🚀 Starting Producer mode")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode,
        acks='all',
        retries=3,
        retry_backoff_ms=1000
    )

    # Отправка тестовых сообщений
    test_orders = [
        {"order_id": "ORD-1001", "customer_id": "CUST-001", "amount": 99.99},
        {"order_id": "ORD-1002", "customer_id": "CUST-002", "amount": 149.50},
        {"order_id": "ORD-1003", "customer_id": "CUST-001", "amount": 75.25},  # Тот же customer
        {"order_id": "ORD-1004", "customer_id": "CUST-003", "amount": -10.00},  # Ошибка
        {"order_id": "ORD-1005", "customer_id": "CUST-004", "amount": 200.00}
    ]

    for order in test_orders:
        message = {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "amount": order["amount"],
            "timestamp": int(time.time()),
            "correlation_id": str(uuid.uuid4()),
            "source": "web-api"
        }

        try:
            logger.info(f"📤 Sending order {order['order_id']} for customer {order['customer_id']}")
            future = producer.send(MAIN_TOPIC, key=order["customer_id"], value=message)
            record_metadata = future.get(timeout=10)
            logger.success(
                f"✅ Order {order['order_id']} sent to partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to send order {order['order_id']}: {e}")

    producer.flush()
    logger.info("🏁 Producer finished")

# ==================== CONSUMER ====================
elif MODE == 'consumer':
    logger.info("👂 Starting Consumer mode")

    consumer = KafkaConsumer(
        MAIN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=10
    )


    def process_message(message):
        """Обработка сообщения"""
        try:
            required_fields = ['order_id', 'customer_id', 'amount']
            if not all(field in message for field in required_fields):
                raise ValueError("Missing required fields")

            if message['amount'] <= 0:
                raise ValueError("Invalid amount")

            logger.info(f"📝 Processing order {message['order_id']} for ${message['amount']}")

            # Имитация обработки
            time.sleep(0.1)

            # ПРОДЮСЕР ВНУТРИ КОНСЬЮМЕРА (Event-driven)
            if message['amount'] > 100:  # Отправить в платежную систему
                payment_producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )

                payment_event = {
                    "order_id": message['order_id'],
                    "amount": message['amount'],
                    "payment_status": "pending",
                    "processed_at": int(time.time())
                }

                payment_producer.send('payment.process', value=payment_event)
                payment_producer.flush()
                logger.info(f"💳 Payment event sent for order {message['order_id']}")
                payment_producer.close()

            return True

        except Exception as e:
            logger.error(f"💥 Processing error for {message.get('order_id', 'unknown')}: {e}")
            return False


    def send_to_dlq(message, error):
        """Отправка в DLQ"""
        try:
            dlq_producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            dlq_message = {
                "original_message": message,
                "error": str(error),
                "timestamp": int(time.time()),
                "retry_count": 0
            }

            dlq_producer.send(DLQ_TOPIC, value=dlq_message)
            dlq_producer.flush()
            logger.warning(f"⚠️ Message sent to DLQ: {DLQ_TOPIC}")
            dlq_producer.close()

        except Exception as dlq_error:
            logger.critical(f"🔥 Failed to send to DLQ: {dlq_error}")


    logger.info("🔄 Starting consumer loop...")
    try:
        for message in consumer:
            logger.info(f"📥 Received from partition {message.partition}, offset {message.offset}")

            success = process_message(message.value)

            if success:
                consumer.commit()
                logger.success(f"✅ Committed offset {message.offset}")
            else:
                send_to_dlq(message.value, "Processing failed")
                # Не коммитим → сообщение будет обработано снова

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    finally:
        consumer.close()
        logger.info("👋 Consumer closed")