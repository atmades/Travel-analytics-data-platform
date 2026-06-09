# .PHONY: up down init test lint compile smoke

# up:
# 	docker compose up -d

# down:
# 	docker compose down

# init:
# 	chmod +x scripts/init_platform.sh
# 	./scripts/init_platform.sh

# test:
# 	python -m pytest tests/ -v

# lint:
# 	python -m ruff check services/ airflow/dags/

# compile:
# 	python -m compileall services/ airflow/dags/

# smoke: lint compile test
