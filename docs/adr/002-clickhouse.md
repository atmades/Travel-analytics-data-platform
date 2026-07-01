# ADR-002: ClickHouse for Analytics Storage

## Context

Analytical workloads require fast aggregations over large datasets.

## Decision

ClickHouse is used as the analytical database.

## Benefits

- Columnar storage
- High-performance aggregations
- Compression

## Trade-offs

- Not intended for OLTP workloads