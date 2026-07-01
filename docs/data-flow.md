# Data Flow

## Booking Pipeline

```text
Booking API
      │
      ▼
Raw Bookings
      │
      ▼
Data Quality
      │
      ▼
dbt Staging
      │
      ▼
Business Marts
```

## Advertising Pipeline

```text
Ads API
    │
    ▼
Raw Ads
    │
    ▼
dbt Staging
    │
    ▼
Ad Performance Mart
```

## User Events

```text
User Events
      │
      ▼
Staging
      │
      ├── Funnel Mart
      └── Booking Conversion Mart
```

## Orders CDC

```text
PostgreSQL
      │
      ▼
Debezium CDC
      │
      ▼
Raw Orders
      │
      ▼
Staging
      │
      ▼
Orders Marts
```