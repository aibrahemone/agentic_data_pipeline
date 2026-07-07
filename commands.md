## Airbyte

https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-2-install-abctl

```sh
abctl local install # run Airbyte
abctl local credentials
```

https://ollama.com/download/mac
check Ollama

```sh
ollama list
ollama pull llama3:latest
```

dbt
`dbt debug`

## Project Setup

```bash
# Clone Github Repo and open in your prefered terminal
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker compose up -d         # starts source Postgres
```

## Environment Variables

Export these in every new terminal session or add it in the profile for the user

```bash
export MOTHERDUCK_TOKEN=<from app.motherduck.com → Settings → Tokens>
export AIRBYTE_CLIENT_ID=<from: abctl local credentials>
export AIRBYTE_CLIENT_SECRET=<from: abctl local credentials>
export FRESHNESS_THRESHOLD_HOURS=24
export ANOMALY_THRESHOLD_PCT=50
```

## First Postgres Table

```sql
-- Connect to Postgres
docker exec -it agentic_pipeline-source-postgres-1 psql -U user -d source_db

-- Create orders table
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_name VARCHAR(100),
  order_value DECIMAL(10,2),
  discount_pct FLOAT,
  created_at TIMESTAMP DEFAULT NOW()
);
-- Historical Data
INSERT INTO orders (customer_name, order_value, discount_pct, created_at)
VALUES ('Alice Johnson', 120.50, 10, NOW() - INTERVAL '7 days'),
('Bob Smith', 310.00, 0, NOW() - INTERVAL '7 days'),
('Carol White', 89.99, 5, NOW() - INTERVAL '7 days'),
('David Brown', 450.00, 15, NOW() - INTERVAL '7 days'),
('Eva Martinez', 230.00, 0, NOW() - INTERVAL '7 days'),
('Alice Johnson', 145.00, 10, NOW() - INTERVAL '6 days'),
('Bob Smith', 280.00, 0, NOW() - INTERVAL '6 days'),
('Carol White', 110.00, 5, NOW() - INTERVAL '6 days'),
('David Brown', 420.75, 15, NOW() - INTERVAL '6 days'),
('Eva Martinez', 195.00, 0, NOW() - INTERVAL '6 days'),
('Frank Lee', 330.00, 0, NOW() - INTERVAL '5 days'),
('Grace Kim', 155.00, 5, NOW() - INTERVAL '5 days'),
('Alice Johnson', 98.50, 10, NOW() - INTERVAL '5 days'),
('Bob Smith', 410.00, 0, NOW() - INTERVAL '5 days'),
('Carol White', 215.00, 5, NOW() - INTERVAL '5 days'),
('David Brown', 180.00, 15, NOW() - INTERVAL '4 days'),
('Eva Martinez', 265.00, 0, NOW() - INTERVAL '4 days'),
('Frank Lee', 390.00, 0, NOW() - INTERVAL '4 days'),
('Grace Kim', 140.00, 5, NOW() - INTERVAL '4 days'),
('Alice Johnson', 220.00, 10, NOW() - INTERVAL '4 days'),
('Bob Smith', 305.00, 0, NOW() - INTERVAL '3 days'),
('Carol White', 78.99, 5, NOW() - INTERVAL '3 days'),
('David Brown', 495.00, 15, NOW() - INTERVAL '3 days'),
('Eva Martinez', 160.00, 0, NOW() - INTERVAL '3 days'),
('Frank Lee', 245.00, 0, NOW() - INTERVAL '3 days'),
('Grace Kim', 290.00, 5, NOW() - INTERVAL '2 days'),
('Alice Johnson', 115.00, 10, NOW() - INTERVAL '2 days'),
('Bob Smith', 355.00, 0, NOW() - INTERVAL '2 days'),
('Carol White', 92.00, 5, NOW() - INTERVAL '2 days'),
('David Brown', 435.00, 15, NOW() - INTERVAL '2 days'),
('Eva Martinez', 200.00, 0, NOW() - INTERVAL '1 day'),
('Frank Lee', 315.00, 0, NOW() - INTERVAL '1 day'),
('Grace Kim', 135.00, 5, NOW() - INTERVAL '1 day'),
('Alice Johnson', 255.00, 10, NOW() - INTERVAL '1 day'),
('Bob Smith', 375.00, 0, NOW() - INTERVAL '1 day');
```

## Airbyte Connection Setup

Configure once in the Airbyte UI at `http://localhost:8000`:

- **Source** — PostgreSQL · host: `host.docker.internal` · port: `5433` · db: `source_db` · user: `user` · password: `password`
- **Destination** — MotherDuck · database: `my_db` · default schema: `source_sync`
- **Schedule** — Manual (agent triggers on demand)

---

## Normal Pipeline Run

```bash
python3.11 agent.py
```

---

## Scenario 1: Self-Healing Column Error

## Scenario 2: Freshness Alert

```bash
export FRESHNESS_THRESHOLD_HOURS=1
python3.11 agent.py
```

---

## Scenario 3: Anomaly Detection

**Setup — Insert an Anomalous Order**

```bash
# Step 1 — insert 7 days of historical baseline data
docker exec -it agentic_pipeline-source-postgres-1 psql -U user -d source_db -c "
INSERT INTO orders (customer_name, order_value, created_at) VALUES
  ('Alice Johnson', 120.50, NOW() - INTERVAL '1 day'),
  ('Bob Smith',     340.00, NOW() - INTERVAL '2 days'),
  ('Carol White',   89.99,  NOW() - INTERVAL '3 days'),
  ('David Brown',   450.75, NOW() - INTERVAL '4 days'),
  ('Eva Martinez',  220.00, NOW() - INTERVAL '5 days'),
  ('Frank Lee',     310.00, NOW() - INTERVAL '6 days'),
  ('Grace Kim',     180.00, NOW() - INTERVAL '7 days');"

# Step 2 — trigger Airbyte sync to land historical rows in MotherDuck

# Step 3 — run dbt to rebuild orders_daily with all dates
dbt run --profiles-dir .

# Step 4 — now insert the anomalous order (today's date)
docker exec -it agentic_pipeline-source-postgres-1 psql -U user -d source_db -c \
  "INSERT INTO orders (customer_name, order_value) VALUES ('Anomaly Test', 999999.99);"

# Step 5 — sync again + run agent
export ANOMALY_THRESHOLD_PCT=30
python3.11 agent.py
```

---

## Scenario 4: New Table Auto-Discovery

```bash
docker exec -it agentic_pipeline-source-postgres-1 \
  psql -U user -d source_db -c "
  CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
  );
  INSERT INTO customers (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith',     'bob@example.com');"
```

---
