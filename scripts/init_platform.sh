#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

wait_for_http() {
    local name="$1"
    local url="$2"
    local max_attempts="${3:-60}"

    echo "Waiting for ${name} at ${url}..."
    for attempt in $(seq 1 "$max_attempts"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            echo "${name} is ready."
            return 0
        fi
        sleep 2
    done

    echo "Timed out waiting for ${name}."
    return 1
}

wait_for_postgres() {
    echo "Waiting for PostgreSQL..."
    for attempt in $(seq 1 60); do
        if docker exec travel_postgres_source pg_isready -U source_user -d source_db >/dev/null 2>&1; then
            echo "PostgreSQL is ready."
            return 0
        fi
        sleep 2
    done

    echo "Timed out waiting for PostgreSQL."
    return 1
}

echo "Starting platform services..."
docker compose up -d

wait_for_http "ClickHouse" "http://localhost:8123/ping"
wait_for_postgres
wait_for_http "Debezium Connect" "http://localhost:8083/connectors"

# echo "Applying ClickHouse schema..."
# docker exec -i travel_clickhouse clickhouse-client \
#     --user travel_user \
#     --password travel_password \
#     --multiquery < sql/init_clickhouse.sql

echo "Seeding PostgreSQL orders table..."
docker exec -i travel_postgres_source psql \
    -U source_user \
    -d source_db < sql/init_postgres.sql

echo "Registering Debezium connector..."
CONNECTOR_PAYLOAD="$(cat debezium/postgres_orders_connector.json)"
CONNECTOR_NAME="$(echo "$CONNECTOR_PAYLOAD" | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")"

if curl -sf "http://localhost:8083/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1; then
    echo "Debezium connector '${CONNECTOR_NAME}' already exists."
else
    curl -sf -X POST "http://localhost:8083/connectors" \
        -H "Content-Type: application/json" \
        -d "$CONNECTOR_PAYLOAD"
    echo "Debezium connector '${CONNECTOR_NAME}' registered."
fi

echo "Producing sample user events..."
docker compose up user_event_producer

echo ""
echo "Platform initialized."
echo "Next steps:"
echo "  - Open Airflow: http://localhost:8081"
echo "  - Trigger DAG: platform_daily_refresh"
echo "  - Inspect ClickHouse: docker exec -it travel_clickhouse clickhouse-client --user travel_user --password travel_password"
