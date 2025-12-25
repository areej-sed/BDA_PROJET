FROM python:3.10-slim

WORKDIR /app

COPY docker/kafka/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/kafka/producer/producer.py .
COPY data/output ./data

CMD ["python", "producer.py"]


