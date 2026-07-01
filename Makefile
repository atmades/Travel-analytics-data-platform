.PHONY: up down init test lint compile smoke

up:
	docker compose up -d

down:
	docker compose down

init:
	chmod +x scripts/init_platform.sh
	./scripts/init_platform.sh

lint:
	python -m ruff check services/ airflow/dags/ shared/ tests/

compile:
	python -m compileall services/ airflow/dags/ shared/

test:
	python -m pytest tests/ -v

smoke: lint compile test