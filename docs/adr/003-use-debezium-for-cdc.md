# ADR 003: Use Debezium for PostgreSQL CDC

## Status

Accepted

## Context

The platform needs to capture changes from the operational `orders` table without adding custom event-publishing logic to the source application.

## Decision

Use Debezium with PostgreSQL logical replication to stream database changes into Kafka.

Debezium publishes order changes to:

```text
orders.public.orders
```


## Alternatives Considered

### Application-level events

Rejected for this project because it would require modifying the source application logic.

### Scheduled batch extraction

Rejected because it would not provide near real-time order state changes.

### Manual database polling

Rejected because it is inefficient and can miss intermediate state changes.

## Consequences

### Positive
Non-invasive integration with PostgreSQL.

- Captures inserts, updates, and deletes.
- Enables near real-time operational analytics.
- Works naturally with Kafka.

### Negative
- Requires understanding of WAL, replication slots, and Debezium payload format.
- Source data types may require special handling.
- CDC topics can expose low-level database changes rather than business-level events.