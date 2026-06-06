# AI Analytics Agent

This service converts natural language questions into ClickHouse SQL using the AI Skills Layer.

## Flow

User question → metadata + prompt templates → LLM → SQL → ClickHouse result

## Setup

Create `.env`:

```env
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=gpt-4.1-mini
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=travel_user
CLICKHOUSE_PASSWORD=travel_password
```

Install dependencies:

```bash
pip install -r ai_agent/requirements.txt
```

Run:

```bash
python ai_agent/main.py
```