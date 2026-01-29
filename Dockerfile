FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование ВСЕХ Python-файлов (включая тестовые)
COPY *.py .

# Копирование .env (если нужно)
COPY .env .

ENV PYTHONUNBUFFERED=1

# Команда по умолчанию — можно менять при запуске
CMD ["python", "producer_consumer.py"]