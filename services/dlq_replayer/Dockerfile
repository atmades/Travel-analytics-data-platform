FROM python:3.11-slim

WORKDIR /app

COPY services/dlq_replayer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/dlq_replayer/main.py .
COPY schemas ./schemas

CMD ["python", "main.py"]