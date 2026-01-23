FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# ← Копируем из корня
COPY producer_consumer.py .

COPY .env .

ENV PYTHONUNBUFFERED=1

CMD ["python", "producer_consumer.py"]