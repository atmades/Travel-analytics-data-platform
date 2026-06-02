# Incident 001: Invalid Magic Byte

## Context

A Kafka consumer failed to deserialize messages after switching from plain JSON payloads to Avro serialization with Schema Registry.

## Symptoms

Consumer logs contained:

```text
ValueDeserializationError
Invalid magic byte
```

No new messages were processed and Kafka offsets stopped advancing.

## Root Cause

The consumer group was reading historical messages that had been produced before Avro serialization was introduced.

Older messages were stored as plain JSON while the consumer expected Confluent Avro format.

## Investigation

1. Verified Kafka topic contained older JSON records.
2. Confirmed producer had been migrated to Avro.
3. Confirmed Schema Registry integration was functioning correctly.
4. Checked consumer group offsets.

## Resolution

A new consumer group was created:

```text
user-event-clickhouse-consumer-observability-v4
```

The new group started reading only Avro-compatible records.

## Lessons Learned

* Schema migrations require compatibility planning.
* Historical topic data may contain multiple serialization formats.
* New consumer groups can be used to isolate incompatible historical data.
* Schema Registry does not automatically solve historical serialization incompatibilities.
