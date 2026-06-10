import importlib.util
from pathlib import Path

import pytest


try:
    from airflow import DAG  # noqa: F401
except ImportError:
    pytest.skip(
        "Apache Airflow is not installed in local test environment",
        allow_module_level=True,
    )


DAGS_DIR = Path("airflow/dags")