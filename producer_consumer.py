# producer_consumer.py
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import uuid
from loguru import logger
import os

# Чтение конфигурации
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
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

    # Отправка тестовых сообщений (включая ошибки)
    test_orders = [
        # Стандартные сообщения
        {"order_id": "ORD-1001", "customer_id": "CUST-001", "amount": 99.99},
        {"order_id": "ORD-1002", "customer_id": "CUST-002", "amount": 149.50},
        {"order_id": "ORD-1003", "customer_id": "CUST-001", "amount": 75.25},  # Тот же customer
        {"order_id": "ORD-1004", "customer_id": "CUST-003", "amount": -10.00},  # Ошибка: отрицательная сумма
        {"order_id": "ORD-1005", "customer_id": "CUST-004", "amount": 200.00},

        # Тест ошибок:
        {"order_id": "ORD-1006", "customer_id": "CUST-005"},  # Ошибка: нет amount
        {"order_id": "ORD-1007", "amount": 50.00},  # Ошибка: нет customer_id
        {"customer_id": "CUST-006", "amount": 60.00},  # Ошибка: нет order_id

        # Новые тестовые сообщения:
        {"order_id": "ORD-1008", "customer_id": "CUST-007", "amount": 0.00},  # Ошибка: amount = 0
        {"order_id": "ORD-1009", "customer_id": "CUST-008", "amount": 100.00},  # Точная граница (не идёт в payment)
        {"order_id": "ORD-1010", "customer_id": "CUST-009", "amount": 100.01},  # Точная граница (идёт в payment)
        {"order_id": "ORD-1011", "customer_id": "CUST-010", "amount": "invalid"},  # Ошибка: не число
        {"order_id": "ORD-1012", "customer_id": "CUST-011", "amount": 300.00},  # Проверка: идёт в payment
    ]

    for order in test_orders:
        message = {
            "order_id": order.get("order_id"),  # Может быть None
            "customer_id": order.get("customer_id"),
            "amount": order.get("amount"),
            "timestamp": int(time.time()),
            "correlation_id": str(uuid.uuid4()),
            "source": "web-api"
        }

        try:
            logger.info(
                f"📤 Sending order {order.get('order_id', 'NO_ORDER_ID')} for customer {order.get('customer_id', 'NO_CUSTOMER_ID')}")
            future = producer.send(MAIN_TOPIC, key=order.get("customer_id", "unknown"), value=message)
            record_metadata = future.get(timeout=10)
            logger.success(
                f"✅ Order {order.get('order_id', 'NO_ORDER_ID')} sent to partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to send order {order.get('order_id', 'NO_ORDER_ID')}: {e}")

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
        # Без десериализации — будем проверять JSON вручную
        value_deserializer=None,
        max_poll_records=10
    )


    def safe_json_decode(data: bytes) -> dict:
        """Безопасное декодирование JSON"""
        try:
            return json.loads(data.decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in message: {data.decode('utf-8', errors='replace')[:100]}... | Error: {e}")
            return {"_invalid_json": True, "raw_data": data.decode('utf-8', errors='replace'), "error": str(e)}


    def validate_message(message_dict: dict) -> tuple[bool, str]:
        """Валидация сообщения"""
        if "_invalid_json" in message_dict:
            return False, "Invalid JSON format"

        required_fields = ['order_id', 'customer_id', 'amount']
        missing_fields = [field for field in required_fields if
                          field not in message_dict or message_dict[field] is None]

        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"

        # Проверка типа amount
        if not isinstance(message_dict['amount'], (int, float)):
            return False, "Amount must be a number"

        if message_dict['amount'] <= 0:
            return False, f"Invalid amount: {message_dict['amount']} (must be > 0)"

        return True, ""


    def process_message(message_value_bytes):
        """Обработка сообщения"""
        try:
            # Декодируем JSON
            message_dict = safe_json_decode(message_value_bytes)

            # Проверяем валидность
            is_valid, error_msg = validate_message(message_dict)
            if not is_valid:
                logger.error(f"❌ Validation failed: {error_msg} | Message: {message_dict}")
                raise ValueError(error_msg)

            # Обработка корректного сообщения
            logger.info(f"📝 Processing order {message_dict['order_id']} for ${message_dict['amount']}")

            # Имитация обработки
            time.sleep(0.1)

            # ПРОДЮСЕР ВНУТРИ КОНСЬЮМЕРА (Event-driven)
            if message_dict['amount'] > 100:  # Отправить в платежную систему
                payment_producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )

                payment_event = {
                    "order_id": message_dict['order_id'],
                    "amount": message_dict['amount'],
                    "payment_status": "pending",
                    "processed_at": int(time.time())
                }

                payment_producer.send('payment.process', value=payment_event)
                payment_producer.flush()
                logger.info(f"💳 Payment event sent for order {message_dict['order_id']}")
                payment_producer.close()

            return True

        except Exception as e:
            logger.error(f"💥 Processing error: {e}")
            return False


    def send_to_dlq(original_message_bytes, error):
        """Отправка в DLQ — исправленная версия"""
        try:
            original_dict = safe_json_decode(original_message_bytes)

            dlq_producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            dlq_message = {
                "original_message": original_dict,
                "error": str(error),
                "timestamp": int(time.time()),
                "correlation_id": str(uuid.uuid4()),  # Уникальный ID для трассировки
                "retry_count": 0
                # Убрали неправильные обращения к dlq_message
            }

            dlq_producer.send(DLQ_TOPIC, value=dlq_message)
            dlq_producer.flush()
            logger.warning(f"✅ Message sent to DLQ: {DLQ_TOPIC} | Error: {error}")
            dlq_producer.close()

        except Exception as dlq_error:
            logger.critical(f"🔥 FAILED to send to DLQ: {dlq_error} | Original error: {error}")


    logger.info("🔄 Starting consumer loop...")
    try:
        for message in consumer:
            logger.info(f"📥 Received from partition {message.partition}, offset {message.offset}")

            success = process_message(message.value)

            if success:
                consumer.commit()
                logger.success(f"✅ Committed offset {message.offset}")
            else:
                send_to_dlq(message.value, "Validation or processing failed")
                # Не коммитим → сообщение будет обработано снова

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    finally:
        consumer.close()
        logger.info("👋 Consumer closed")