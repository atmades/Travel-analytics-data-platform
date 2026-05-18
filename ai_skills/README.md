# AI Skills Layer

This folder describes the semantic layer used by AI assistants and agents.

The goal is to help an LLM understand:
- available analytics tables
- business meaning of metrics
- correct SQL generation patterns
- how to answer common travel analytics questions

## Metadata files

- `metadata/tables.yaml` — table and column descriptions
- `metadata/metrics.yaml` — business metrics and calculation logic

## Example questions

- What is the total revenue by route?
- How many paid bookings do we have per day?
- What is the booking status distribution?
- Which transport type generates the most revenue?