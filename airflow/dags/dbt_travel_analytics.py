"""
Run dbt transformations for the Travel Analytics platform.

Purpose:
- Execute dbt models after raw ingestion pipelines are completed.
- Run dbt data tests to validate analytical models.
- Keep transformation SQL, lineage, and tests inside dbt instead of Airflow.

Execution:
- dbt run builds staging and mart models.
- dbt test validates model-level data quality rules.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/project/dbt/travel_analytics"
DBT_TARGET_DIR = "/tmp/dbt/travel_analytics/target"
DBT_LOG_PATH = "/tmp/dbt/travel_analytics/logs"



dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=(
        f"mkdir -p {DBT_TARGET_DIR} {DBT_LOG_PATH} && "
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt test --profiles-dir . "
        f"--target-path {DBT_TARGET_DIR} "
        f"--log-path {DBT_LOG_PATH}"
    ),
)


with DAG(
    dag_id="dbt_travel_analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["travel", "dbt", "analytics-engineering"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"mkdir -p {DBT_TARGET_DIR} {DBT_LOG_PATH} && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir . "
            f"--target docker "
            f"--target-path {DBT_TARGET_DIR} "
            f"--log-path {DBT_LOG_PATH}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"mkdir -p {DBT_TARGET_DIR} {DBT_LOG_PATH} && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir . "
            f"--target docker "
            f"--target-path {DBT_TARGET_DIR} "
            f"--log-path {DBT_LOG_PATH}"
        ),
    )

    dbt_run >> dbt_test