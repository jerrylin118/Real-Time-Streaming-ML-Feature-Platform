FROM python:3.11-slim

WORKDIR /app

COPY services/stream_processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schemas /app/schemas
COPY services/stream_processor /app/processor
WORKDIR /app/processor

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
