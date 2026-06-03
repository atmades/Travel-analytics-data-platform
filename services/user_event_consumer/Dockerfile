FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# COPY main.py .
# COPY config.py .
# COPY metrics.py .
# COPY clickhouse_client.py .
# COPY dlq.py .
# COPY utils.py .
# COPY contracts ./contracts

CMD ["python", "main.py"]