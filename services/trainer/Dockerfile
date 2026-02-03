FROM python:3.11-slim

WORKDIR /app

COPY services/trainer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schemas /app/schemas
COPY services/stream_processor/feature_defs.py /app/feature_defs.py
COPY services/trainer /app/trainer
WORKDIR /app/trainer

ENV PYTHONPATH=/app

CMD ["python", "train.py"]
