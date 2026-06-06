# Runbook

Operational guide for the Travel Analytics Data Platform.

---

# Local Environment Startup

## Start Services


```bash
docker compose up -d
```

## Rebuild Specific Service

```bash
docker compose up --build -d mock_ads_api
```

## Check Running Containers

```bash
docker ps
```

--- 


# Airflow Operations


## Open Airflow UI
http://localhost:8080


## Restart Airflow

```bash
docker compose restart airflow
```

## Common Airflow Troubleshooting

### DAG not visible

Check:

- file location;
- Python import errors;
- Airflow logs.

Useful command:

```bash
docker logs travel_airflow
```

### Task failed

Inspect:

- task logs;
- ClickHouse connectivity;
- API availability;
- schema validation errors.

---

# ClickHouse Operations

## Open ClickHouse Client

```bash
docker exec -it travel_clickhouse clickhouse-client \
  --user travel_user \
  --password travel_password
```

## Check Tables

```sql
SHOW TABLES FROM travel;
```

## Inspect Raw Payloads

```sql
SELECT *
FROM travel.raw_ads_api_payloads
ORDER BY fetched_at DESC
LIMIT 10;
```

## Inspect Staging Data

```sql
SELECT *
FROM travel.stg_ads
LIMIT 10;
```

## Inspect Marts

```sql
SELECT *
FROM travel.stg_ads
LIMIT 10;
```

# Mock APIs

## Booking API

Endpoint:
http://localhost:8001/bookings

Check:

```bash
curl http://localhost:8001/bookings
```

## Ads API

Endpoints:

http://localhost:8002/google-ads
http://localhost:8002/meta-ads

Check:

```bash
curl http://localhost:8002/google-ads
curl http://localhost:8002/meta-ads
```

---


# Adapter Troubleshooting

## Missing credentials

Exception:
- MissingCredentialsError

Typical causes:
- missing .env variables;
- invalid ADS_API_MODE configuration.


## API connectivity problems

Exception:
- ExternalApiError

Typical causes:
- API unavailable;
- timeout;
- invalid token;
- rate limit.

## Validation problems

Exception:
- AdapterValidationError

Typical causes:
- invalid schema;
- negative metrics;
- missing required fields.


---


# Docker Troubleshooting

## Service does not start

Check logs:

```bash
docker logs <container_name>
```

## Rebuild from scratch
```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

# Environment Variables

Configuration template:
- .env.example

Secrets should never be committed to Git.

# Future Operational Components

Planned:
- Kafka monitoring;
- MinIO health checks;
- CDC monitoring;
- OpenTelemetry metrics;
- Grafana dashboards.