FROM python:3.11-slim

WORKDIR /app

COPY services/user_event_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/user_event_producer/main.py .
COPY schemas ./schemas

CMD ["python", "main.py"]
