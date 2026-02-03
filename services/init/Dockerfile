FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.3.0
COPY scripts/init_kafka.py .

CMD ["python", "init_kafka.py"]
