# Data Flow

This document describes how data moves through the Travel Analytics Data Platform.

---

# Core Principles

The platform follows:

- immutable raw ingestion;
- append-only raw layers;
- staged transformations;
- schema validation;
- replayability;
- separation of concerns.

---

# Booking Data Flow

## Flow Overview

```text
Mock Booking API
        ↓
Airflow ingestion DAG
        ↓
raw_bookings
        ↓
Validation / deduplication
        ↓
stg_bookings
        ↓
Business marts
```

## Raw Layer

Table:

- raw_bookings

Purpose:

- append-only ingestion;
- preserve ingestion history;
- support replay/debugging.

Characteristics:

- may contain duplicates;
- may contain invalid records;
- ingestion-oriented schema.


## Staging Layer

Table:

- stg_bookings

Purpose:

- validated records;
- deduplicated data;
- business-ready schema.

Characteristics:

- canonical representation;
- deduplication applied;
- invalid records excluded.


## Marts

Examples:

- mart_daily_bookings
- mart_route_revenue
- mart_booking_status

Purpose:

- business analytics;
- AI-agent queries;
- reporting.


---


### Advertising Data Flow

## Flow Overview

Mock/External Ads APIs
        ↓
Adapter Layer
        ↓
Immutable Raw Payloads
        ↓
Validation / normalization
        ↓
raw_google_ads / raw_meta_ads
        ↓
stg_ads
        ↓
mart_ad_performance


## Adapter Layer

Responsibilities:

- API integration;
- credential handling;
- retries;
- normalization;
- validation;
- schema contracts.

Current adapters:

- GoogleAdsAdapter
- MetaAdsAdapter


## Immutable Raw Payload Layer

Table:

- raw_ads_api_payloads

Purpose:

- preserve original API responses;
- replay/backfill support;
- auditability;
- debugging.

Characteristics:

- append-only;
- immutable;
- stores original payload JSON.


## Raw Ads Tables

Tables:

- raw_google_ads
- raw_meta_ads

Purpose:

- normalized ingestion layer;
- source-specific raw storage.


## Staging Ads Layer

Table:

- stg_ads

Purpose:

- validated business-ready advertising data.


## Advertising Mart

Table:

- mart_ad_performance

Metrics:

- CTR
- CPC
- campaign spend
- impressions
- clicks

### Validation Flow

Validation occurs after immutable raw payload persistence.

Validation categories:

## Configuration Validation

Examples:

- missing credentials;
- invalid API mode.

Exceptions:

- MissingCredentialsError


## Connectivity Validation

Examples:

- API timeout;
- authentication failure;
- rate limit.

Exceptions:

- ExternalApiError


## Schema Validation

Examples:

- missing fields;
- invalid types;
- negative metrics.

Exceptions:

- AdapterValidationError


---


### Planned Future Flows

## Kafka Event Streaming

Planned topics:

- bookings.events
- user.events
- ads.events

Planned architecture:

Producers
    ↓
Kafka
    ↓
Consumers
    ↓
ClickHouse

## CDC Ingestion

Planned:

- Debezium
- orders.cdc
- payments.cdc


## MinIO Raw Archive

Future architecture:

External APIs
      ↓
MinIO raw archive
      ↓
Replay/backfill
      ↓
Normalization
      ↓
ClickHouse