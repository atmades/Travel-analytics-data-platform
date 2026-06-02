# ADR 001: Use Kafka for Event Streaming

## Status

Accepted

## Context

The platform needs to process user behavior events and CDC events from operational systems in near real time.

The system must support:

- asynchronous communication between producers and consumers;
- replayable event streams;
- consumer groups;
- decoupling between ingestion and processing;
- future extension for additional consumers.

## Decision

Use Apache Kafka as the central event streaming backbone.

Kafka is used for:

- user behavior events: `user.events.avro`;
- CDC events from PostgreSQL via Debezium: `orders.public.orders`.

## Alternatives Considered

### Direct API calls

Rejected because they tightly couple producers and consumers and do not provide replay or durable event storage.

### Database polling

Rejected because it increases load on the source database and does not provide real-time event delivery.

### Message queue only

Rejected because classic queues are not ideal for replayable analytics streams.

## Consequences

### Positive

- Producers and consumers are decoupled.
- Events can be replayed.
- Multiple consumers can read the same topic independently.
- Kafka UI and consumer lag monitoring can be used for operations.

### Negative

- Adds operational complexity.
- Requires careful schema and compatibility management.
- Requires monitoring of consumer lag and failed messages.