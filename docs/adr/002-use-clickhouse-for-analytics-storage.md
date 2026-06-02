# ADR 002: Use ClickHouse for Analytics Storage

## Status

Accepted

## Context

The platform needs fast analytical queries over bookings, user events, marketing metrics, CDC data, and business marts.

The workload is analytical rather than transactional.

## Decision

Use ClickHouse as the analytical storage layer.

ClickHouse stores:

- raw ingestion tables;
- staging tables;
- business marts;
- DLQ tables;
- data quality results.

## Alternatives Considered

### PostgreSQL

Rejected as the primary analytics database because the expected workload is columnar analytical aggregation rather than transactional processing.

### BigQuery / Snowflake

Not used in this local project to keep the platform fully runnable with Docker Compose.

### Data lake only

Rejected for the current stage because the project needs fast SQL analytics and iterative mart development.

## Consequences

### Positive

- Fast analytical queries.
- Good fit for event and metrics data.
- Simple local deployment.
- Works well with Airflow-driven transformations.

### Negative

- Not a transactional source of truth.
- Some mutations, such as updating replay status, are less natural than in OLTP databases.
- Requires attention to table engines and ordering keys.