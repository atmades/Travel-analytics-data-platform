# Architecture

## Overview

The Travel Analytics Data Platform is an end-to-end analytics platform built around a layered architecture.

Data is ingested from multiple sources, validated, transformed with dbt, and exposed through analytics marts.

## Technology Stack

- Airflow
- Python
- ClickHouse
- dbt
- Kafka
- Debezium CDC
- Schema Registry
- Docker

## Architecture

```mermaid
flowchart LR

subgraph Sources
A[Booking API]
B[Ads API]
C[User Events]
D[PostgreSQL CDC]
end

subgraph Raw
R1[(raw_bookings)]
R2[(raw_ads)]
R3[(raw_user_events)]
R4[(raw_orders)]
end

subgraph Data Quality
DQ1[DQ Checks]
end

subgraph dbt
STG[Staging]
MART[Business Marts]
end

subgraph Monitoring
OBS[Metrics & DQ]
end

A --> R1
B --> R2
C --> R3
D --> R4

R1 --> DQ1
R2 --> STG
R3 --> STG
R4 --> STG

DQ1 --> STG
STG --> MART
MART --> OBS
```

## Layers

### Raw

Immutable landing zone.

### Data Quality

Freshness, completeness and uniqueness validation.

### Staging

Business-ready normalized datasets built with dbt.

### Marts

Business metrics optimized for BI and analytics.