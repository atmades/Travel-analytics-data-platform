# ADR-004: Avro and Schema Registry

## Context

Independent producers and consumers require stable contracts.

## Decision

Events use Avro schemas managed by Schema Registry.

## Benefits

- Schema evolution
- Strong typing
- Backward compatibility

## Trade-offs

- Additional infrastructure component
- Schema governance required