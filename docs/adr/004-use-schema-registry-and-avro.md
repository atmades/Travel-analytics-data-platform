# ADR 004: Use Schema Registry and Avro for User Events

## Status

Accepted

## Context

User behavior events are consumed by downstream services and analytical pipelines.

Plain JSON does not provide a strong contract, versioning, or compatibility checks.

## Decision

Use Avro and Schema Registry for the `user.events.avro` Kafka topic.

The event contract is stored in:

```text
schemas/avro/user_event.avsc
```

The Schema Registry subject is:

```text
user.events-value
```

## Alternatives Considered

### Plain JSON

Rejected because schema changes are hard to control and consumers can break silently.

### Protobuf

Considered, but Avro was chosen because it integrates well with Confluent Schema Registry and Kafka data pipelines.

## #Debezium Avro for CDC

Deferred to a later milestone because it requires a Kafka Connect image with Avro converter support.

## Consequences

### Positive
- Strong event contracts.
- Schema evolution support.
- Compatibility checks.
- Safer producer-consumer integration.

### Negative
- More complex than plain JSON.
- Requires Schema Registry availability.
- Historical JSON messages cannot be consumed by Avro consumers without migration.