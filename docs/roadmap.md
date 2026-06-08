# Roadmap

This document describes the evolution plan of the Travel Analytics Data Platform.

---

# Vision

The platform is designed as a production-style modern data platform combining:

- batch ingestion;
- event-driven architecture;
- immutable raw storage;
- analytical warehouse patterns;
- AI-enabled analytics;
- replayable ingestion pipelines.

The goal is to practice architecture and engineering patterns commonly used in modern Data Engineering and Data Platform teams.

---

# Completed Versions

## v0.1.0

Initial ClickHouse setup.

Features:

- local ClickHouse deployment;
- base schema;
- initial ingestion experiments.

---

## v0.2.0

Booking ingestion pipelines.

Features:

- mock booking API;
- raw_bookings ingestion;
- staging layer;
- marts.

---

## v0.3.0

Data marts and analytics.

Features:

- mart_daily_bookings;
- mart_route_revenue;
- mart_booking_status.

---

## v0.4.0

Validation and DLQ concepts.

Features:

- duplicate handling;
- validation logic;
- DLQ patterns.

---

## v0.5.0

AI analytics layer foundation.

Features:

- AI agent;
- metadata-driven SQL generation;
- AI Skills structure.

---

## v0.6.0

Advertising analytics foundation.

Features:

- mock advertising APIs;
- ads ingestion;
- advertising marts.

---

## v0.7.0

Production-style adapter layer.

Features:

- adapter abstraction;
- centralized settings;
- enum-based configuration;
- custom exceptions;
- schema validation.

---

## v0.8.0

Immutable raw payload architecture.

Features:

- immutable raw payload ingestion;
- append-only payload storage;
- payload replay foundation;
- Pydantic validation;
- operational documentation.

---

## v0.9.0 — Kafka Event Streaming

Features:

- Kafka + Zookeeper;
- Avro schema registry;
- user event producer/consumer;
- DLQ and replay workflow;
- event ingestion into ClickHouse.

Architecture:

Event Producers → Kafka → Consumers → ClickHouse


## v1.0.0 — CDC Layer

Features:

- Debezium PostgreSQL connector;
- `orders.public.orders` CDC topic;
- CDC consumer with infrastructure retry semantics;
- staging and order analytics marts.

Goals:

- database change streams;
- replayable CDC ingestion;
- campaign attribution joins.


## v1.1.0 — Platform Engineering

Features:

- complete schema-as-code bootstrap;
- `make init` one-command local setup;
- shared ClickHouse client for Airflow DAGs;
- adapter-driven ads ingestion;
- `platform_daily_refresh` orchestration DAG;
- pytest unit test suite;
- GitHub Actions quality gate.


# Planned Versions

## v1.2.0 — MinIO Lakehouse Layer

Planned features:

- MinIO raw archive;
- immutable object storage;
- replay/backfill support;
- historical payload archive.

Future architecture:

External APIs
      ↓
MinIO
      ↓
Normalization
      ↓
ClickHouse


## v1.3.0 — Observability Layer

Planned features:

- Grafana dashboards;
- Prometheus alert rules;
- OpenTelemetry;
- consumer lag monitoring;
- DAG SLA monitoring.


## v1.4.0 — Metadata & Catalog Layer

Planned features:

- data contracts;
- lineage;
- metadata catalog;
- schema registry concepts.

Potential technologies:

- OpenMetadata;
- DataHub;
- OpenLineage.


# Long-Term Goals

The project aims to simulate engineering challenges common for:

- Data Platform teams;
- Analytics Engineering teams;
- Event-driven architectures;
- AI-enabled data systems;
- Streaming platforms.

The platform is intentionally designed to evolve incrementally toward production-grade architecture patterns.