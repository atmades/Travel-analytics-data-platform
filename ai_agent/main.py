import os
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]

TABLES_PATH = ROOT_DIR / "ai_skills" / "metadata" / "tables.yaml"
METRICS_PATH = ROOT_DIR / "ai_skills" / "metadata" / "metrics.yaml"
SQL_SKILL_PATH = ROOT_DIR / "ai_skills" / "prompts" / "sql_generation_skill.md"

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "travel_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "travel_password")


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def build_prompt(question: str) -> str:
    tables = load_yaml(TABLES_PATH)
    metrics = load_yaml(METRICS_PATH)
    sql_skill = load_text(SQL_SKILL_PATH)

    return f"""
{sql_skill}

## Tables metadata

{yaml.dump(tables, allow_unicode=True)}

## Metrics metadata

{yaml.dump(metrics, allow_unicode=True)}

## User question

{question}

Return only SQL. Do not include markdown.
"""


def generate_sql(question: str) -> str:
    # Тестовый режим без OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not found. Using mock SQL generation.")

        # ✅ Все if на одном уровне — последовательные проверки
        if "top routes" in question.lower():
            return """
SELECT
    route,
    transport_type,
    total_revenue
FROM travel.mart_route_revenue
ORDER BY total_revenue DESC
LIMIT 10
"""

        if "status" in question.lower():
            return """
SELECT
    status,
    total_bookings
FROM travel.mart_booking_status
ORDER BY total_bookings DESC
"""

        if "paid bookings" in question.lower():
            return """
SELECT
    booking_date,
    paid_bookings
FROM travel.mart_daily_bookings
ORDER BY booking_date
"""

        if "dlq" in question.lower():
            return """
SELECT count()
FROM travel.dlq_bookings
"""

        # Дефолтный запрос, если ни одно условие не сработало
        return """
SELECT
    booking_date,
    total_bookings,
    total_revenue
FROM travel.mart_daily_bookings
ORDER BY booking_date
"""

    # ✅ Реальный вызов OpenAI — выполняется, если API-ключ ЕСТЬ
    client = OpenAI()
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    response = client.responses.create(
        model=model,
        input=build_prompt(question),
    )

    return response.output_text.strip()

def run_clickhouse_query(query: str) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=20,
    )

    if not response.ok:
        raise Exception(response.text)

    return response.text.strip()



def main():

    question = input("Ask a travel analytics question: ")

    sql = generate_sql(question)
    print("\nGenerated SQL:")
    print(sql)

    result = run_clickhouse_query(sql)
    print("\nResult:")
    print(result)



if __name__ == "__main__":
    main()

