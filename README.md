# AI Data Pipeline Agent

An on-premise AI agent that monitors, heals, and extends your data pipeline automatically using LangGraph, dbt, Airbyte, and a local LLM (Llama 3 via Ollama).

> Built as part of a YouTube tutorial — [watch the video](https://www.youtube.com/watch?v=cUwdNaNKbdw)

---

## What It Does

The agent runs after every Airbyte sync and handles four scenarios without human intervention:

**Self-healing** — dbt fails on a missing or renamed column. The agent queries MotherDuck for the live schema, asks the LLM to identify the correct column with a confidence score, and patches the SQL file using Python. dbt retries automatically.

**Freshness monitoring** — checks when the last record arrived from Postgres. If data is older than the configured threshold, the LLM writes a plain-English diagnosis with a recommended action.

**Anomaly detection** — compares today's metrics against a 7-day rolling average. If revenue or order count deviates beyond the threshold, the LLM classifies the anomaly and reasons about root cause.

**New table discovery** — scans MotherDuck for source tables without a dbt staging model. The LLM generates a complete `stg_*.sql` file with null-handling rules and updates `schema.yml` automatically.

---

## Architecture

```
PostgreSQL
    -> Airbyte (abctl) -> MotherDuck (my_db)
                              ├── source_sync  (raw tables)
                              ├── staging      (dbt views)
                              └── marts        (dbt tables)
                                      -> LangGraph Agent
                                              -> Ollama / Llama 3 (local)
```

---

## Stack

| Component | Tool |
|---|---|
| Data source | PostgreSQL 15 (Docker) |
| Ingestion | Airbyte via abctl |
| Warehouse | MotherDuck (free tier) |
| Transformation | dbt-duckdb 1.10.0 |
| Agent framework | LangGraph 0.2.28 |
| Local LLM | Ollama + Llama 3 (native, not Docker) |
| Language | Python 3.11+ |

---

## Prerequisites

- Python 3.11+
- Docker Desktop
- [Ollama](https://ollama.com) installed natively
- [abctl](https://github.com/airbytehq/abctl) — `brew install airbytehq/tap/abctl`
- [MotherDuck account](https://app.motherduck.com) (free tier)

---

## Setup

```bash
# Clone and install
git clone https://github.com/<your-username>/agentic_pipeline.git
cd agentic_pipeline
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Pull the LLM model
ollama pull llama3:latest

# Start Airbyte and Postgres
abctl local install
docker-compose up -d

# Set environment variables
cp .env.example .env
# Edit .env with your MOTHERDUCK_TOKEN and Airbyte credentials
export $(cat .env | xargs)
```

Configure Airbyte at `http://localhost:8000`:
1. Source: PostgreSQL — host `host.docker.internal`, port `5433`, db `source_db`, user `user`, password `password`
2. Destination: MotherDuck — your token, database `my_db`, schema `source_sync`
3. Create the connection — schedule: Manual

```bash
# Run dbt to set up models
dbt run --profiles-dir .

# Run the agent
python3.11 agent.py
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MOTHERDUCK_TOKEN` | required | MotherDuck auth token |
| `MOTHERDUCK_DB` | `my_db` | MotherDuck database name |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama endpoint |
| `FRESHNESS_THRESHOLD_HOURS` | `24` | Hours before data is considered stale |
| `ANOMALY_THRESHOLD_PCT` | `50` | Deviation % to trigger anomaly alert |
| `RUN_LOG_PATH` | `pipeline_runs.json` | Path to run history log |

---

## Project Structure

```
agentic_pipeline/
├── agent.py                     # Main AI agent
├── webhook_server.py            # Optional Airbyte webhook receiver
├── docker-compose.yml           # Runs source Postgres
├── requirements.txt
├── dbt_project.yml
├── profiles.yml                 # dbt -> MotherDuck connection
├── macros/
│   └── generate_schema_name.sql # Prevents dbt prepending main_ to schemas
├── models/
│   ├── schema.yml               # All sources and models in one file
│   ├── staging/
│   │   └── stg_orders.sql
│   └── marts/
│       └── orders_daily.sql
└── pipeline_runs.json           # Auto-created run history
```
---

## License

MIT
