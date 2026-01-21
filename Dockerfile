FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY src/ ./src/

# Для немедленного вывода логов в Docker
ENV PYTHONUNBUFFERED=1

# Запуск по умолчанию (можно переопределить)
CMD ["python", "-c", "print('Use: docker run kafka-qa-demo python src/producer.py')"]