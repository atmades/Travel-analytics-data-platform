# Travel Analytics Data Platform

A portfolio Data Engineering project that demonstrates the design and implementation of a modern analytics platform for a travel business.

The platform ingests data from multiple sources, including booking APIs, advertising platforms, user events, and database change data capture (CDC). Data is processed through layered storage (Raw → Staging → Marts), validated with Data Quality checks, and exposed through analytical data marts.

Local Airflow uses SequentialExecutor; master DAG triggers domain DAGs asynchronously.
In production, this can be switched to wait_for_completion=True with Local/Celery/KubernetesExecutor.

```text
Bookings   → Marts
Ads        → Marts
Events     → Marts
Orders CDC → Marts

Marts → DQ Monitoring Mart
```

## Key Engineering Concepts Demonstrated

* Event-Driven Architecture
* Change Data Capture (CDC)
* Kafka & Schema Registry
* Avro Data Contracts
* DLQ & Replay Patterns
* Layered Data Platform Design
* Data Quality Validation
* ClickHouse Analytics
* Airflow Orchestration
* Observability & Monitoring
* At-Least-Once Delivery Semantics
* Recovery & Replay Workflows


## Technology Stack

| Category               | Technologies              |
|------------------------|---------------------------|
| Programming            | Python                    |
| Workflow Orchestration | Airflow                   |
| Streaming              | Kafka                     |
| CDC                    | Debezium                  |
| Schema Management      | Confluent Schema Registry |
| Storage                | ClickHouse                |
| Source Database        | PostgreSQL                |
| Containerization       | Docker, Docker Compose    |
| Monitoring             | Prometheus                |
| Data Contracts         | Avro, Pydantic            |
| Data Quality           | Custom DQ Framework       |


## Developer Experience

The project includes several developer productivity features:

* GitHub Actions CI pipeline
* Ruff code quality checks
* Unit tests with Pytest
* One-command platform initialization (`make init`)
* One-command validation suite (`make smoke`)
* Automated Debezium connector registration


## Architecture Patterns

- Batch Processing
- Event-Driven Processing
- Change Data Capture (CDC)
- Dead Letter Queue (DLQ)
- Replay & Recovery
- Layered Data Platform Architecture
- At-Least-Once Delivery


## Key Features

### Batch Data Pipelines

* Booking API ingestion into ClickHouse
* Advertising platform ingestion (Google Ads / Meta Ads)
* Airflow orchestration
* Data Quality validation
* Incremental transformations
* Analytical marts generation

### Event-Driven Data Processing

* Kafka-based user event streaming
* Avro schemas and Schema Registry
* CDC ingestion with Debezium
* Dead Letter Queue (DLQ)
* DLQ replay and recovery workflow
* At-least-once delivery semantics

### Data Platform Capabilities

* Layered architecture (Raw → Staging → Marts)
* Data Quality framework
* Observability and metrics with Prometheus
* Recovery and replay mechanisms
* Consumer retry strategies
* CDC processing pipeline


## Architecture Overview

The platform combines multiple data ingestion patterns commonly used in modern data platforms:

* Batch ingestion from APIs
* Event-driven streaming with Kafka
* Change Data Capture (CDC) with Debezium
* Layered storage architecture (Raw → Staging → Marts)
* Data Quality validation
* DLQ replay and recovery workflows

## Local Setup

### Prerequisites

- Docker
- Docker Compose
- Python 3.11+

### Developer Dependencies

```bash
pip install -r requirements-dev.txt
```

### Clone Repository

```bash
git clone <repository-url>
cd travel-analytics-data-platform
```

### Initialize Platform

```bash
make init 
```

This command:

* Starts all platform services
* Waits for ClickHouse, PostgreSQL, and Debezium to become available
* Applies ClickHouse schema
* Seeds PostgreSQL source data
* Registers the Debezium CDC connector
* Produces sample user events

### Run Quality Checks

```bash
make smoke
```

This command runs:

* Ruff linting
* Python compilation checks
* Unit tests

### Useful Commands

```bash
make up
make down
make lint
make compile
make test
```

### Access Components

| Component  | URL                   |
| ---------- | --------------------- |
| Airflow    | http://localhost:8081 |
| ClickHouse | http://localhost:8123 |
| Kafka UI   | http://localhost:8088 |
| Prometheus | http://localhost:9090 |
| Grafana    | http://localhost:3000 |



## Data Sources

The platform integrates multiple types of data sources that are commonly found in modern travel and e-commerce companies.

### Booking API

A mock booking service simulates operational booking transactions.

Example attributes:

* booking_id
* user_id
* route
* transport_type
* status
* price
* currency
* created_at

The data is ingested through Airflow and stored in the Raw layer before further transformations.

### User Events

User behavior events are produced through Kafka and serialized using Avro schemas.

Example events:

* search_performed
* booking_started
* payment_completed

The streaming pipeline demonstrates:

* Kafka producers and consumers
* Schema Registry integration
* Avro serialization
* DLQ handling
* Replay and recovery workflows

### Orders CDC

Order changes are captured from PostgreSQL using Debezium CDC.

Captured operations:

* INSERT
* UPDATE
* DELETE

CDC events are published into Kafka topics and persisted into ClickHouse for downstream analytics.

### Advertising Platforms

The project simulates advertising data ingestion from multiple marketing platforms.

Sources:

* Google Ads
* Meta Ads

Advertising data is normalized into a unified schema and later transformed into analytical marts for campaign performance analysis.

## Data Platform Layers

The platform follows a layered architecture approach that separates raw ingestion from business-ready analytics.

```text
Sources
    ↓
Raw Layer
    ↓
Staging Layer
    ↓
Data Quality Validation
    ↓
Data Marts
```

### Raw Layer

The Raw layer stores data as close as possible to the source systems.
Raw layer is append-only and stores all ingestion runs.

Objectives:

* Preserve original source information
* Support replay and backfill operations
* Enable troubleshooting and auditing
* Provide a single source of truth

Examples:

* raw_bookings
* raw_user_events
* raw_cdc_orders
* raw_google_ads
* raw_meta_ads
* raw_ads_api_payloads

Characteristics:

* Minimal transformations
* Immutable event storage where possible
* Ingestion timestamp tracking
* Source system traceability

### Staging Layer

The Staging layer standardizes and prepares data for analytical consumption.
Staging layer deduplicates bookings using argMax(..., loaded_at) and keeps the latest version per booking_id.

Objectives:

* Normalize source-specific structures
* Apply business rules
* Standardize field names and formats
* Remove source-specific complexity

Examples:

* stg_bookings
* stg_ads

Characteristics:

* Consistent naming conventions
* Type normalization
* Data enrichment
* Business-ready schemas

### Data Quality Layer

Before data reaches analytical marts, quality checks are executed.

Validation examples:

* Completeness checks
* Uniqueness checks
* Freshness checks
* Schema validation

Invalid data is isolated and investigated before downstream consumption.

### Analytical Marts

Marts contain business-oriented datasets optimized for reporting and analytics.

Examples:

* mart_booking_performance
* mart_ad_performance
* mart_daily_revenue
* mart_route_popularity

Characteristics:

* Aggregated metrics
* Business-friendly structure
* Fast analytical queries
* Consumption-ready datasets

## Event-Driven Processing

The platform includes a Kafka-based event processing pipeline for user activity tracking.

### Components

* Kafka
* Schema Registry
* Avro Schemas
* User Event Producer
* User Event Consumer
* Dead Letter Queue (DLQ)
* DLQ Replayer

### Delivery Guarantees

The consumer implements **at-least-once delivery semantics**.

Offsets are committed only after successful persistence to ClickHouse.

### Failure Handling

The platform distinguishes between:

* Invalid events → routed to DLQ
* Infrastructure failures → retried without offset commit

### DLQ Replay

Failed events can be replayed from the DLQ.

The replay workflow tracks:

* replay_count
* last_replay_at
* recovered
* recovered_at

This provides full visibility into recovery attempts and successful event restoration.

## CDC Processing

The platform uses Change Data Capture (CDC) to capture operational database changes in near real-time.

### Components

* PostgreSQL
* Debezium
* Kafka
* CDC Consumer
* ClickHouse

### Supported Operations

* INSERT
* UPDATE
* DELETE

### Processing Flow

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
CDC Consumer
    ↓
ClickHouse
```

CDC events are persisted into the Raw layer and can be used for downstream analytics, auditing, and historical reconstruction.

## Data Quality

The platform includes automated Data Quality validation before analytical consumption.

### Implemented Checks

* Completeness
* Uniqueness
* Freshness

### Examples

* Raw tables must contain data
* Business keys must be unique
* Data must arrive within expected time windows

Failed checks stop downstream processing and surface issues early in the pipeline.

## Observability


The platform includes Prometheus and Grafana for monitoring data ingestion, event processing, CDC pipelines, and platform health.

Kafka consumer lag is monitored through `kafka_exporter`.

### Prometheus scrape targets

- `prometheus`
- `user_event_consumer`
- `orders_cdc_consumer`
- `kafka_exporter`

### Grafana provisioning

Grafana is provisioned automatically with:

- Prometheus datasource
- Travel Data Platform Overview dashboard

### Dashboard metrics

The dashboard provides visibility into:

- User events received, processed, failed, and sent to DLQ
- CDC events received, processed, and failed
- Kafka consumer lag by consumer group

### Key metrics

- `events_received_total`
- `events_processed_total`
- `events_failed_total`
- `events_sent_to_dlq_total`
- `cdc_events_received_total`
- `cdc_events_processed_total`
- `cdc_events_failed_total`
- `kafka_consumergroup_lag`

### Example PromQL queries

```promql
increase(events_processed_total[5m])
```

```promql
sum(kafka_consumergroup_lag) by (consumergroup)
```

### Verification

Prometheus targets:

```text
http://localhost:9090/targets
```

Expected targets:

- prometheus
- user_event_consumer
- orders_cdc_consumer
- kafka_exporter


### Tracked Metrics

* Events received
* Events processed
* Events failed
* Events sent to DLQ

These metrics provide visibility into pipeline health and processing behavior.

### Alerting

Grafana alert rules are provisioned automatically.

Current alert rules:

- `User Events Failed Last 5m`
  - triggers when `increase(events_failed_total[5m]) > 0`

- `Kafka Consumer Lag High`
  - triggers when `sum(kafka_consumergroup_lag) by (consumergroup) > 100`

Alert rules are defined in:

```text
monitoring/grafana/provisioning/alerting/alerts.yml
```

## Project Structure

The project follows a layered architecture approach.

```text
services/
├── user_event_producer/
├── user_event_consumer/
├── orders_cdc_consumer/
├── adapters/
└── ...

docs/
├── architecture/
├── services/
├── decisions/
└── incidents/
```

Key design principles:

* Separation of business logic and infrastructure
* Explicit data contracts
* Modular service boundaries
* Recoverability and replay support

## Local Setup

### Prerequisites

* Docker
* Docker Compose
* Git

### Clone Repository

```bash
git clone <repository-url>
cd travel-analytics-data-platform
```

### Start Platform

```bash
docker compose up -d
```

### Verify Services

```bash
docker ps
```

### Access Components

| Component          | URL                           |
| ------------------ | ----------------------------- |
| Airflow            | http://localhost:8080         |
| ClickHouse         | http://localhost:8123         |
| Prometheus Metrics | http://localhost:8010/metrics |

### Run Example Pipelines

Booking API ingestion:

```bash
docker compose up booking_api
```

User event streaming:

```bash
docker compose up user_event_producer
```

DLQ replay:

```bash
docker compose up dlq_replayer
```

### Development Setup

```bash
pip install -r requirements-dev.txt
```

Service dependencies are pinned to ensure reproducible builds.

## Future Improvements

Potential next steps for the platform:

* GitHub Actions CI/CD pipeline
* Automated testing
* Great Expectations integration
* Grafana dashboards
* S3/MinIO data lake layer
* dbt transformations
* Incremental CDC snapshots
* Data catalog and lineage tracking
* Lakehouse architecture patterns
* Multi-environment deployment support
* Replay attempt limits
* Automated DLQ recovery workflows
* Consumer lag monitoring
* End-to-end integration tests
* Refactor stg_user_events to ReplacingMergeTree(event_id, ingested_at)

