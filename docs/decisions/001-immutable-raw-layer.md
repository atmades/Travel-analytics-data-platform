# ADR-001: Immutable Raw Payload Layer

## Status

Accepted

---

# Context

The platform integrates with external APIs such as advertising platforms.

External APIs may:

- change schemas;
- return invalid records;
- experience temporary failures;
- require replay/backfill.

Originally, records were validated before insertion into raw tables.

This approach reduced data quality issues in raw tables but lost original API payloads.

---

# Decision

The platform will store original API payloads in an immutable append-only raw layer before normalization and validation.

Current implementation stores payloads in:

- travel.raw_ads_api_payloads

Future versions may move immutable raw payload storage to MinIO/S3.

Validation and normalization happen after raw payload persistence.

---

# Alternatives Considered

## Validate before raw ingestion

Pros:

- cleaner raw tables;
- simpler downstream logic.

Cons:

- loss of original payload;
- limited replayability;
- difficult debugging.

---

## Direct API → staging

Rejected because:

- no immutable history;
- weak auditability;
- difficult schema evolution handling.

---

# Consequences

Positive:

- replay support;
- auditability;
- easier debugging;
- schema evolution support;
- future lakehouse compatibility.

Trade-offs:

- additional storage usage;
- more ingestion complexity.

---

# Related Components

Tables:

- raw_ads_api_payloads
- raw_google_ads
- raw_meta_ads

Services:

- mock_ads_api
- adapters

DAGs:

- ads_api_to_clickhouse

---

# Related Documents

- architecture.md
- data_flow.md
- roadmap.md