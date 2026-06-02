# Incident 002: Debezium Decimal Encoding

## Context

A CDC pipeline was implemented using PostgreSQL, Debezium, Kafka, and ClickHouse.

Order data was captured from PostgreSQL and published to Kafka through Debezium. A consumer then loaded CDC events into ClickHouse staging and analytics layers.

## Symptoms

Revenue-related marts started producing incorrect values.

Inspection of raw CDC messages showed the following payload:

```json
{
  "price": "EBg=",
  "currency": "USD"
}
```

Expected value:

```json
{
  "price": 77.5,
  "currency": "USD"
}
```

As a result:

* Revenue metrics were incorrect.
* Route revenue marts contained zero values.
* Aggregations based on price could not be trusted.

## Root Cause

The PostgreSQL column:

```sql
price NUMERIC(10,2)
```

PostgreSQL NUMERIC was serialized by Debezium JSON converter as encoded decimal value.


## Investigation

1. Verified PostgreSQL source data contained correct numeric values.
2. Inspected Kafka CDC messages.
3. Identified Base64-encoded decimal payloads such as:

```text
EBg=
```

4. Reviewed Debezium documentation regarding Decimal Logical Types.
5. Confirmed consumer lacked decimal decoding logic.

## Resolution

For the local training project, the source column type was changed from NUMERIC(10,2) to DOUBLE PRECISION to simplify CDC JSON processing.

In production, a better solution would be either:

1. implement Debezium decimal logical type decoding, or
2. configure Debezium decimal handling mode, or
3. use Avro/Schema Registry with proper logical type support.

This is acceptable for a local training project, but production financial systems should usually keep DECIMAL/NUMERIC precision.

