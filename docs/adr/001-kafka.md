# ADR-001: Kafka for Event Streaming

## Context

The platform processes multiple independent event streams.

## Decision

Kafka is used as the event backbone.

## Benefits

- Decoupled services
- Replay support
- Horizontal scalability

## Trade-offs

- Additional operational complexity