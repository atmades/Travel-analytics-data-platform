# Data Contracts

This document describes canonical schemas and validation contracts used across the Travel Analytics Data Platform.

The platform follows contract-first ingestion principles.

---

# Principles

Data contracts define:

- required fields;
- field types;
- semantic expectations;
- validation rules;
- ownership boundaries.

Contracts help:

- reduce schema drift;
- improve replayability;
- support event-driven systems;
- improve downstream stability.

---

# Advertising Metrics Contract

## Canonical Schema

```text
platform
campaign_id
campaign_name
impressions
clicks
spend
```

## Field Definitions

| Field         | Type   | Description           |
|---------------|--------|-----------------------|
| platform      | String | Advertising platform  |
| campaign_id   | Int    | Campaign identifier   |
| campaign_name | String | Campaign display name |
| impressions   | Int    | Total impressions     |
| clicks        | Int    | Total clicks          |
| spend         | Float  | Advertising spend     |

---

# Validation Rules

## platform

Allowed values:

- google_ads
- meta_ads

## campaign_name

Rules:

- cannot be empty;
- cannot contain only whitespace.

## impressions / clicks / spend

Rules:

- must be non-negative.


# Validation Layer

## Validation implementation:

- services/adapters/ads/validators.py

Validation technology:

- Pydantic

Validation exceptions:

- AdapterValidationError

# Immutable Raw Payload Contract

Table:

- raw_ads_api_payloads

Purpose:

- preserve original API responses;
- support replay;
- support debugging;
- preserve source-of-truth payloads.

Characteristics:

- append-only;
- immutable;
- stores raw JSON payloads.

# Booking Contract

## Canonical Fields

- booking_id
- user_id
- route
- transport_type
- status
- price
- booking_date

---

# Planned Event Contracts

## user.events

Planned schema:

- event_id
- user_id
- event_type
- event_time
- properties

## bookings.events

Planned schema:

- event_id
- booking_id
- status
- price
- event_time

## ads.events

Planned schema:

- event_id
- platform
- campaign_id
- metric_type
- metric_value
- event_time


# Future Improvements

Planned:

- Avro schemas;
- JSON Schema registry;
- Kafka schema governance;
- schema evolution tracking;
- compatibility validation.

Potential technologies:

- Schema Registry;
- OpenMetadata;
- DataHub.