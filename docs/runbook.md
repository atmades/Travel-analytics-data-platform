# Runbook

## Start Platform

```bash
docker compose up -d
```

## Initialize

```bash
make init
```

## Run dbt

```bash
dbt run
```

## Run Tests

```bash
dbt test
```

## Trigger Full Refresh

```bash
airflow dags trigger platform_daily_refresh
```

## Restart Airflow

```bash
docker compose restart airflow
```

---

## Common Issues

### Scheduler not running

```bash
airflow jobs check --job-type SchedulerJob
```

Restart Airflow if no scheduler is alive.

### DAG not visible

```bash
airflow dags list-import-errors
```

### dbt compilation errors

Verify:

- profiles.yml
- dbt_project.yml
- adapter versions