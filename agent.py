import json
import os
import re
import subprocess
import time
from datetime import datetime
from typing import TypedDict, Literal

import duckdb
from langchain_community.llms import Ollama
from langgraph.graph import StateGraph, END

# ── Config ────────────────────────────────────────────────────────────────────
OLLAMA_URL            = os.getenv("OLLAMA_URL", "http://localhost:11434")
MOTHERDUCK_TOKEN      = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DB         = os.getenv("MOTHERDUCK_DB", "my_db")
DBT_PROJECT           = os.getenv("DBT_PROJECT_PATH", "./")
DBT_MODELS_DIR        = os.path.join(DBT_PROJECT, "models")
STAGING_DIR           = os.path.join(DBT_MODELS_DIR, "staging")
DBT_TABLE             = "orders"
SOURCE_SCHEMA         = "source_sync"  # Airbyte raw tables land here
MARTS_SCHEMA          = "marts"    # dbt output tables land here
DBT_SCHEMA            = SOURCE_SCHEMA  # kept for backwards compat
FRESHNESS_THRESHOLD   = int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "24"))
ANOMALY_THRESHOLD_PCT = float(os.getenv("ANOMALY_THRESHOLD_PCT", "50"))
RUN_LOG_PATH          = os.getenv("RUN_LOG_PATH", "pipeline_runs.json")

llm = Ollama(model="llama3:latest", base_url=OLLAMA_URL)


# ── State ─────────────────────────────────────────────────────────────────────
class PipelineState(TypedDict):
    dbt_output:      str
    dbt_run_output:  str
    dbt_test_output: str
    dbt_success:     bool
    validation:      dict
    retry_count:     int
    final_status:    str
    new_tables:      list   # tables found in MotherDuck without a dbt model


# ── MotherDuck helpers ────────────────────────────────────────────────────────
def md_connect():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")


def find_model_file(model_name: str) -> str | None:
    """Find a dbt model's .sql file anywhere under models/ (staging/, marts/, etc)."""
    for root, dirs, files in os.walk(DBT_MODELS_DIR):
        for f in files:
            if f == f"{model_name}.sql":
                return os.path.join(root, f)
    return None


def infer_source_table_for_model(model_name: str, model_sql: str) -> str:
    """
    Best-effort guess of which raw source table a model reads from.
    1. If the model name starts with stg_, the table is the suffix (stg_customers -> customers)
    2. Otherwise look for source('airbyte_source', 'TABLE') in the SQL
    3. Otherwise look for ref('stg_TABLE') and strip the stg_ prefix
    """
    if model_name.startswith("stg_"):
        return model_name[len("stg_"):]

    src_match = re.search(r"source\(\s*[\'\"]airbyte_source[\'\"]\s*,\s*[\'\"](\w+)[\'\"]", model_sql)
    if src_match:
        return src_match.group(1)

    ref_match = re.search(r"ref\(\s*[\'\"]stg_(\w+)[\'\"]", model_sql)
    if ref_match:
        return ref_match.group(1)

    return DBT_TABLE  # fallback


def get_motherduck_context(table: str = DBT_TABLE, schema: str = DBT_SCHEMA) -> dict:
    context = {"columns": [], "sample": [], "error": None}
    try:
        con = md_connect()
        describe = con.execute(f"DESCRIBE {schema}.{table}").fetchall()
        context["columns"] = [{"name": row[0], "type": row[1]} for row in describe]
        sample = con.execute(f"SELECT * FROM {schema}.{table} LIMIT 3").fetchall()
        col_names = [row[0] for row in describe]
        context["sample"] = [dict(zip(col_names, row)) for row in sample]
        con.close()
        print(f"   MotherDuck columns in {schema}.{table}:")
        for col in context["columns"]:
            print(f"     {col['name']} ({col['type']})")
    except Exception as e:
        context["error"] = str(e)
        print(f"   Could not query MotherDuck: {e}")
    return context


# ── Node 1: Run dbt ───────────────────────────────────────────────────────────
def run_dbt(state: PipelineState) -> PipelineState:
    print("🔧  Running dbt models + tests...")

    run_result = subprocess.run(
        ["dbt", "run", "--profiles-dir", DBT_PROJECT, "--project-dir", DBT_PROJECT],
        capture_output=True, text=True
    )
    test_result = subprocess.run(
        ["dbt", "test", "--profiles-dir", DBT_PROJECT, "--project-dir", DBT_PROJECT],
        capture_output=True, text=True
    )

    run_out  = run_result.stdout + run_result.stderr
    test_out = test_result.stdout + test_result.stderr

    state["dbt_run_output"]  = run_out
    state["dbt_test_output"] = test_out
    state["dbt_output"]      = "=== dbt run ===\n" + run_out + "\n=== dbt test ===\n" + test_out
    state["dbt_success"]     = (run_result.returncode == 0 and test_result.returncode == 0)

    print(f"   dbt run  exit code: {run_result.returncode}")
    print(f"   dbt test exit code: {test_result.returncode}")
    print(f"   dbt success: {state['dbt_success']}")

    if run_result.returncode != 0:
        print("   dbt run errors:\n" + run_out[-1500:])
    if test_result.returncode != 0:
        print("   dbt test errors:\n" + test_out[-1500:])

    return state


# ── Node 2: Validate with LLM ─────────────────────────────────────────────────
def validate_data(state: PipelineState) -> PipelineState:
    print("🤖  Analyzing dbt failure...")

    if state["dbt_success"]:
        state["validation"] = {
            "status": "ok", "reason": "All tests passed.",
            "fix": None, "bad_col": None, "correct_col": None
        }
        return state

    dbt_run_out  = state.get("dbt_run_output",  state["dbt_output"])
    dbt_test_out = state.get("dbt_test_output", "")

    # ── Identify WHICH model actually failed ────────────────────────────────
    # dbt run output has lines like:
    #   Failure in model orders_daily (models/marts/orders_daily.sql)
    failed_model_match = re.search(r"Failure in model (\w+) \(([^)]+)\)", dbt_run_out)
    if failed_model_match:
        failed_model_name = failed_model_match.group(1)
        failed_model_relpath = failed_model_match.group(2)   # e.g. models/marts/orders_daily.sql
        dbt_model_path = os.path.join(DBT_PROJECT, failed_model_relpath)
        print(f"   Failed model detected: {failed_model_name}  →  {dbt_model_path}")
    else:
        # Fall back to searching by filename anywhere under models/
        failed_model_name = None
        dbt_model_path = None

    try:
        with open(dbt_model_path, "r") as f:
            current_model = f.read()
    except (FileNotFoundError, TypeError):
        current_model = ""
        dbt_model_path = None

    # Detect which layer failed
    run_ok  = "Completed successfully" in dbt_run_out
    test_ok = dbt_test_out == "" or "Completed successfully" in dbt_test_out

    # Case: dbt run passed but tests failed
    if run_ok and not test_ok:
        print("   Error type: dbt run passed but tests FAILED")
        failed_tests = re.findall(r"FAIL\s+(\S+)", dbt_test_out)
        error_msgs   = re.findall(r"Failure in test (\S+)", dbt_test_out)
        print(f"   Failed tests: {failed_tests or error_msgs}")

        test_prompt = (
            f"You are a senior data engineer. dbt model ran successfully but these data tests failed:\n\n"
            f"Failed tests: {failed_tests or error_msgs}\n\n"
            f"dbt test output (last 1500 chars):\n{dbt_test_out[-1500:]}\n\n"
            f"In 2-3 sentences explain:\n"
            f"1. What data quality issue these failures indicate\n"
            f"2. What the on-call engineer should check first\n"
            f"Be specific and actionable. Plain text only."
        )
        print("   🤖  Asking LLM to diagnose test failures...")
        llm_diagnosis = llm.invoke(test_prompt).strip()
        print(f"   🤖  LLM: {llm_diagnosis}")

        state["validation"] = {
            "status": "escalate",
            "reason": f"dbt tests failed: {failed_tests or error_msgs}",
            "llm_diagnosis": llm_diagnosis,
            "fix": None, "bad_col": None, "correct_col": None
        }
        return state

    # Detect error type
    dict_error   = "'dict' object has no attribute" in dbt_run_out
    source_match = re.search(r"source named [\w.\"'`]+ which was not found",
                             dbt_run_out, re.IGNORECASE)

    # DuckDB Binder Error patterns
    col_match = None
    for pattern in [
        r'Referenced column "(\w+)" not found',
        r'column "(\w+)" not found',
        r'column (\w+) does not exist',
        r'no such column[:\s]+"?(\w+)"?',
    ]:
        col_match = re.search(pattern, dbt_run_out, re.IGNORECASE)
        if col_match:
            print(f"   Matched error pattern: {pattern}")
            break

    bad_col = col_match.group(1) if col_match else None
    print(f"   Bad column detected: {bad_col}")

    # Case 0: dbt version bug
    if dict_error:
        state["validation"] = {
            "status": "escalate",
            "reason": "dbt test syntax error — upgrade dbt-duckdb",
            "fix": None, "bad_col": None, "correct_col": None
        }
        return state

    # Case 1: Column not found
    if bad_col:
        print(f"   Error type: missing column '{bad_col}'")

        # Figure out the correct source table + schema for THIS failing model
        target_table  = infer_source_table_for_model(failed_model_name or "", current_model)
        target_schema = SOURCE_SCHEMA if (failed_model_name or "").startswith("stg_") else MARTS_SCHEMA
        # If the failing model is itself a staging model, its source lives in SOURCE_SCHEMA.
        # If it's a marts model (reads via ref()), the upstream staging view lives in 'staging'.
        if (failed_model_name or "").startswith("stg_"):
            target_schema = SOURCE_SCHEMA
        else:
            # marts model failing on a column — the candidate columns actually live in
            # the staging view it reads from (via ref()), so look there instead of raw source
            target_schema = "staging"
            ref_match = re.search(r"ref\(\s*['\"](stg_\w+)['\"]", current_model)
            if ref_match:
                target_table = ref_match.group(1)

        print(f"   Looking up live schema for: {target_schema}.{target_table}")
        md = get_motherduck_context(target_table, target_schema)

        if md["error"]:
            state["validation"] = {
                "status": "escalate",
                "reason": f"Column '{bad_col}' not found. Could not reach MotherDuck: {md['error']}",
                "fix": None, "bad_col": bad_col, "correct_col": None
            }
            return state

        schema_info = "\n".join([f"  - {c['name']} ({c['type']})" for c in md["columns"]])
        sample_rows = ""
        for row in md["sample"]:
            sample_rows += "  " + ", ".join([f"{k}={v}" for k, v in row.items()]) + "\n"

        numeric_types = {"decimal", "double", "float", "integer", "int", "bigint", "numeric", "real"}
        numeric_cols = [
            c["name"] for c in md["columns"]
            if any(t in c["type"].lower() for t in numeric_types)
            and not c["name"].startswith("_airbyte")
        ]

        # Ask LLM for the correct column AND a confidence score
        prompt = (
            f'A dbt SQL model references column "{bad_col}" which does not exist.\n\n'
            f"Actual columns in the table (live from MotherDuck):\n{schema_info}\n\n"
            f"Sample data (3 rows):\n{sample_rows}\n"
            f"Numeric columns (most likely candidates): {numeric_cols}\n\n"
            f'Which single column is the correct replacement for "{bad_col}"?\n'
            f"Reply in this exact format (two lines only):\n"
            f"column: <column_name>\n"
            f"confidence: <0-100>\n\n"
            f"confidence = 100 means you are certain. confidence < 60 means you are guessing."
        )

        response = llm.invoke(prompt).strip()
        print(f"   LLM response: {response}")

        # Parse column name and confidence from response
        correct_col  = None
        confidence   = 0
        all_cols = [c["name"] for c in md["columns"] if not c["name"].startswith("_airbyte")]

        for line in response.splitlines():
            line = line.strip().lower()
            if line.startswith("column:"):
                candidate = line.split(":", 1)[1].strip()
                # Find the best matching real column name
                for col in sorted(all_cols, key=len, reverse=True):
                    if col.lower() in candidate:
                        correct_col = col
                        break
            if line.startswith("confidence:"):
                try:
                    confidence = int(re.search(r"\d+", line).group())
                except Exception:
                    confidence = 0

        # Fallback: search entire response if structured parse failed
        if not correct_col:
            for col in sorted(all_cols, key=len, reverse=True):
                if col.lower() in response.lower():
                    correct_col = col
                    confidence  = confidence or 50
                    break

        print(f"   Correct column: {correct_col}   Confidence: {confidence}%")

        CONFIDENCE_THRESHOLD = 70

        if not correct_col or confidence < CONFIDENCE_THRESHOLD:
            reason = (
                f"Column '{bad_col}' not found. "
                + (f"LLM suggested '{correct_col}' but confidence is only {confidence}% (threshold: {CONFIDENCE_THRESHOLD}%). "
                   if correct_col else "LLM could not identify a replacement. ")
                + f"Candidates by data type: {numeric_cols}"
            )
            print(f"   ⚠️  Confidence too low to auto-fix — escalating")
            state["validation"] = {
                "status": "escalate",
                "reason": reason,
                "fix": None, "bad_col": bad_col, "correct_col": correct_col,
                "confidence": confidence,
            }
            return state

        print(f"   ✅  Confidence {confidence}% ≥ {CONFIDENCE_THRESHOLD}% — proceeding with auto-fix")
        state["validation"] = {
            "status": "fixable",
            "reason": f"Column '{bad_col}' replaced with '{correct_col}' (confidence: {confidence}%)",
            "fix": "column_rename",
            "bad_col": bad_col,
            "correct_col": correct_col,
            "confidence": confidence,
            "model_path": dbt_model_path,
            "model_name": failed_model_name,
        }
        return state

    # Case 2: Source not found
    if source_match:
        state["validation"] = {
            "status": "escalate",
            "reason": "dbt source config error — source name in SQL does not match schema.yml",
            "fix": None, "bad_col": None, "correct_col": None
        }
        return state

    # Case 3: Unknown
    state["validation"] = {
        "status": "escalate",
        "reason": f"Unknown dbt error:\n{dbt_run_out[-500:].strip()}",
        "fix": None, "bad_col": None, "correct_col": None
    }
    return state


# ── Node 3: Alert ─────────────────────────────────────────────────────────────
def alert_team(state: PipelineState) -> PipelineState:
    v           = state["validation"]
    reason      = v.get("reason", "Unknown error")
    bad_col     = v.get("bad_col")
    correct_col = v.get("correct_col")

    print("\n" + "=" * 60)
    print("🚨  PIPELINE ESCALATION REPORT")
    print("=" * 60)
    print(f"\n📋  Root Cause:\n    {reason}")

    if bad_col and correct_col:
        print(f"\n🔍  Column Analysis:")
        print(f"    Referenced in SQL : {bad_col}")
        print(f"    Best candidate    : {correct_col}")
        print(f"    Action needed     : Manually verify and update orders_daily.sql")
    elif bad_col:
        print(f"\n🔍  Column Analysis:")
        print(f"    Referenced in SQL : {bad_col}")
        print(f"    Could not determine correct replacement automatically")

    print("\n📝  Suggested Next Steps:")
    print("    1. Run: SELECT * FROM main.orders LIMIT 5  in MotherDuck")
    print("    2. Update models/orders_daily.sql with correct column name")
    print("    3. Update models/schema.yml tests to match")
    print("    4. Re-run: python3.11 agent.py")
    print("\n" + "=" * 60)

    state["final_status"] = "escalated"
    return state


# ── Node 4: Self-Heal ─────────────────────────────────────────────────────────
def heal_pipeline(state: PipelineState) -> PipelineState:
    print("🔨  Attempting auto-heal...")

    bad_col     = state["validation"].get("bad_col")
    correct_col = state["validation"].get("correct_col")

    if not bad_col or not correct_col:
        print("   No column mapping available. Escalating.")
        state["validation"]["status"] = "escalate"
        return state

    state["retry_count"] = state.get("retry_count", 0) + 1
    if state["retry_count"] > 3:
        print("   Max retries reached. Escalating.")
        state["validation"]["status"] = "escalate"
        return state

    dbt_model_path = state["validation"].get("model_path")
    schema_path    = os.path.join(DBT_MODELS_DIR, "schema.yml")

    if not dbt_model_path or not os.path.exists(dbt_model_path):
        print(f"   Could not locate failing model file ({dbt_model_path}). Escalating.")
        state["validation"]["status"] = "escalate"
        return state

    print(f"   Target file: {dbt_model_path}")

    any_fixed = False
    for filepath, label in [(dbt_model_path, os.path.basename(dbt_model_path)), (schema_path, "schema.yml")]:
        try:
            with open(filepath, "r") as f:
                original = f.read()

            if bad_col not in original:
                print(f"   '{bad_col}' not found in {label} — skipping")
                continue

            with open(filepath + ".bak", "w") as f:
                f.write(original)

            # Whole-word replacement — prevents partial matches e.g.
            # replacing "value" inside "order_value" → "order_order_value"
            fixed = re.sub(r'\b' + re.escape(bad_col) + r'\b', correct_col, original)
            with open(filepath, "w") as f:
                f.write(fixed)

            print(f"   ✓ {label}: '{bad_col}' → '{correct_col}'")
            any_fixed = True

        except Exception as e:
            print(f"   Failed to update {label}: {e}")
            state["validation"]["status"] = "escalate"
            return state

    if not any_fixed:
        print(f"   '{bad_col}' not found in any file. Escalating.")
        state["validation"]["status"] = "escalate"

    return state


# ── Node 5: dbt Success ───────────────────────────────────────────────────────
def pipeline_success(state: PipelineState) -> PipelineState:
    print("\n✅  dbt models and tests passed — running health checks...")
    state["final_status"] = "success"
    return state


# ── Node 6: Freshness Check (Scenario 4) ─────────────────────────────────────
def check_freshness(state: PipelineState) -> PipelineState:
    """
    Scenario 4 — Freshness Monitoring:
    Check if the most recent record in MotherDuck is older than the threshold.
    Alerts if data is stale so the team knows to trigger a new Airbyte sync.
    """
    print("\n🕐  Checking data freshness...")

    try:
        con = md_connect()
        result = con.execute(f"""
            SELECT
                MAX(created_at) AS last_record,
                EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600 AS age_hours
            FROM {SOURCE_SCHEMA}.{DBT_TABLE}
        """).fetchone()
        con.close()

        last_record = result[0]
        age_hours   = float(result[1]) if result[1] else 9999

        print(f"   Last record timestamp : {last_record}")
        print(f"   Data age              : {age_hours:.1f} hours")
        print(f"   Freshness threshold   : {FRESHNESS_THRESHOLD} hours")

        if age_hours > FRESHNESS_THRESHOLD:
            print(f"   ⚠️  Data is STALE — {age_hours:.1f}h old (threshold: {FRESHNESS_THRESHOLD}h)")

            # Ask LLM to narrate the freshness issue like a data engineer would
            print("   🤖  Asking LLM to diagnose freshness issue...")
            fresh_prompt = (
                f"You are a senior data engineer reviewing a data pipeline health report.\n\n"
                f"FRESHNESS ALERT:\n"
                f"  Table: {DBT_SCHEMA}.{DBT_TABLE}\n"
                f"  Last record: {last_record}\n"
                f"  Data age: {age_hours:.1f} hours\n"
                f"  Threshold: {FRESHNESS_THRESHOLD} hours\n\n"
                f"In 2-3 sentences, explain:\n"
                f"1. What this means for downstream dashboards and reports\n"
                f"2. The most likely cause (Airbyte sync failure, upstream DB issue, etc.)\n"
                f"3. The recommended immediate action\n\n"
                f"Be direct and specific. No bullet points. Plain text only."
            )
            llm_diagnosis = llm.invoke(fresh_prompt).strip()
            print(f"   🤖  LLM Diagnosis: {llm_diagnosis}")

            state["validation"]["freshness"] = {
                "status": "stale",
                "age_hours": round(age_hours, 1),
                "last_record": str(last_record),
                "threshold_hours": FRESHNESS_THRESHOLD,
                "message": f"Data is {age_hours:.1f}h old — trigger a new Airbyte sync",
                "llm_diagnosis": llm_diagnosis,
            }
        else:
            print(f"   ✅  Data is FRESH — {age_hours:.1f}h old")
            state["validation"]["freshness"] = {
                "status": "fresh",
                "age_hours": round(age_hours, 1),
                "last_record": str(last_record),
            }

    except Exception as e:
        print(f"   Could not check freshness: {e}")
        state["validation"]["freshness"] = {"status": "error", "message": str(e)}

    return state


# ── Node 7: Anomaly Detection (Scenario 1) ────────────────────────────────────
def detect_anomalies(state: PipelineState) -> PipelineState:
    """
    Scenario 1 — Data Quality Anomaly Detection:
    Compare today's revenue and order count vs 7-day rolling average.
    Flags deviations above the configured threshold percentage.
    """
    print("\n🔍  Running anomaly detection...")

    try:
        con = md_connect()
        # Use the most recent date in orders_daily rather than CURRENT_DATE
        # This handles timezone mismatches and cases where today's dbt run
        # hasn't happened yet
        latest_date_row = con.execute(f"""
            SELECT MAX(order_date) FROM {MARTS_SCHEMA}.orders_daily
        """).fetchone()
        con.close()

        latest_date = latest_date_row[0] if latest_date_row else None

        if not latest_date:
            print("   No data in orders_daily yet — skipping anomaly check")
            state["validation"]["anomalies"] = {"status": "no_data"}
            return state

        print(f"   Latest date in orders_daily: {latest_date}")

        con2 = md_connect()
        metrics = con2.execute(f"""
            WITH latest AS (
                SELECT total_orders, revenue
                FROM {MARTS_SCHEMA}.orders_daily
                WHERE order_date = '{latest_date}'
            ),
            baseline AS (
                SELECT
                    AVG(total_orders) AS avg_orders,
                    AVG(revenue)      AS avg_revenue
                FROM {MARTS_SCHEMA}.orders_daily
                WHERE order_date >= '{latest_date}'::DATE - INTERVAL 7 DAY
                  AND order_date <  '{latest_date}'
            )
            SELECT
                l.total_orders,
                l.revenue,
                b.avg_orders,
                b.avg_revenue,
                CASE WHEN b.avg_orders > 0
                     THEN ABS(l.total_orders - b.avg_orders) / b.avg_orders * 100
                     ELSE 0 END AS order_dev_pct,
                CASE WHEN b.avg_revenue > 0
                     THEN ABS(l.revenue - b.avg_revenue) / b.avg_revenue * 100
                     ELSE 0 END AS revenue_dev_pct
            FROM latest l, baseline b
        """).fetchone()
        con2.close()

        if not metrics or metrics[0] is None:
            print(f"   No data for {latest_date} — skipping anomaly check")
            state["validation"]["anomalies"] = {"status": "no_data"}
            return state

        today_orders    = metrics[0]
        today_revenue   = float(metrics[1]) if metrics[1] is not None else 0.0
        avg_orders      = float(metrics[2]) if metrics[2] is not None else 0.0
        avg_revenue     = float(metrics[3]) if metrics[3] is not None else 0.0
        order_dev_pct   = float(metrics[4]) if metrics[4] is not None else 0.0
        revenue_dev_pct = float(metrics[5]) if metrics[5] is not None else 0.0

        # No baseline history — can't do meaningful comparison
        if avg_orders == 0.0 and avg_revenue == 0.0:
            print("   ⚠️  Insufficient history for baseline — need at least 1 prior day of data")
            print(f"   Today: {today_orders} orders, ${today_revenue:.2f} revenue")
            print("   Tip: insert historical orders with past created_at timestamps and re-sync")
            state["validation"]["anomalies"] = {
                "status": "no_baseline",
                "today_orders": today_orders,
                "today_revenue": today_revenue,
                "message": "No historical data available for comparison. Run with at least 1 prior day of data."
            }
            return state

        print(f"   Latest ({latest_date}) → orders: {today_orders}   revenue: ${today_revenue:.2f}")
        print(f"   7d avg → orders: {avg_orders:.1f}  revenue: ${avg_revenue:.2f}")
        print(f"   Deviation → orders: {order_dev_pct:.1f}%   revenue: {revenue_dev_pct:.1f}%")

        anomalies = []
        if order_dev_pct > ANOMALY_THRESHOLD_PCT:
            anomalies.append(
                f"Order count deviation {order_dev_pct:.1f}% "
                f"(today={today_orders}, 7d_avg={avg_orders:.1f})"
            )
        if revenue_dev_pct > ANOMALY_THRESHOLD_PCT:
            anomalies.append(
                f"Revenue deviation {revenue_dev_pct:.1f}% "
                f"(today=${today_revenue:.2f}, 7d_avg=${avg_revenue:.2f})"
            )

        if anomalies:
            print("   🚨  ANOMALIES DETECTED:")
            for a in anomalies:
                print(f"      - {a}")

            # Ask LLM to reason about why this anomaly occurred
            print("   🤖  Asking LLM to reason about anomalies...")
            anomaly_prompt = (
                f"You are a senior data engineer investigating a data anomaly alert.\n\n"
                f"ANOMALY REPORT:\n"
                f"  Today's orders : {today_orders} (7-day avg: {avg_orders:.1f}, "
                f"deviation: {order_dev_pct:.1f}%)\n"
                f"  Today's revenue: ${today_revenue:.2f} (7-day avg: ${avg_revenue:.2f}, "
                f"deviation: {revenue_dev_pct:.1f}%)\n"
                f"  Anomalies found: {anomalies}\n\n"
                f"In 3-4 sentences explain:\n"
                f"1. What kind of anomaly this is (spike, drop, or both)\n"
                f"2. The 2 most likely root causes in a real e-commerce pipeline\n"
                f"3. What the data engineer should check first\n"
                f"4. Whether this looks like a data quality issue or a real business event\n\n"
                f"Be specific and actionable. Plain text, no bullet points."
            )
            llm_reasoning = llm.invoke(anomaly_prompt).strip()
            print(f"   🤖  LLM Reasoning: {llm_reasoning}")

            state["validation"]["anomalies"] = {
                "status": "anomaly_detected",
                "anomalies": anomalies,
                "today_orders": today_orders,
                "today_revenue": today_revenue,
                "avg_orders": avg_orders,
                "avg_revenue": avg_revenue,
                "threshold_pct": ANOMALY_THRESHOLD_PCT,
                "llm_reasoning": llm_reasoning,
            }
        else:
            print("   ✅  No anomalies — data looks normal")
            state["validation"]["anomalies"] = {
                "status": "ok",
                "today_orders": today_orders,
                "today_revenue": today_revenue,
            }

    except Exception as e:
        print(f"   Could not run anomaly detection: {e}")
        state["validation"]["anomalies"] = {"status": "error", "message": str(e)}

    return state


# ── Node 8: Final Report ──────────────────────────────────────────────────────
def final_report(state: PipelineState) -> PipelineState:
    freshness = state["validation"].get("freshness", {})
    anomalies = state["validation"].get("anomalies", {})

    print("\n" + "=" * 60)
    print("📊  PIPELINE HEALTH REPORT")
    print("=" * 60)

    print("\n✅  dbt models & tests  : PASSED")

    # Freshness
    f_status = freshness.get("status", "unknown")
    if f_status == "fresh":
        print(f"✅  Data freshness      : FRESH ({freshness.get('age_hours')}h old)")
    elif f_status == "stale":
        print(f"⚠️   Data freshness      : STALE ({freshness.get('age_hours')}h old)")
        print(f"    → {freshness.get('message')}")
    else:
        print(f"⚠️   Data freshness      : {f_status}")

    # Anomalies
    a_status = anomalies.get("status", "unknown")
    if a_status == "ok":
        print(f"✅  Anomaly detection   : CLEAN")
        print(f"    Today: {anomalies.get('today_orders')} orders  "
              f"${anomalies.get('today_revenue', 0):.2f} revenue")
    elif a_status == "anomaly_detected":
        print("🚨  Anomaly detection   : ANOMALIES FOUND")
        for a in anomalies.get("anomalies", []):
            print(f"    → {a}")
    elif a_status == "no_data":
        print("ℹ️   Anomaly detection   : No data for today yet")
    elif a_status == "no_baseline":
        print(f"⚠️   Anomaly detection   : No baseline history")
        print(f"    Today: {anomalies.get('today_orders')} orders  ${anomalies.get('today_revenue',0):.2f} revenue")
        print(f"    → {anomalies.get('message')}")
    else:
        print(f"⚠️   Anomaly detection   : {a_status}")

    # New tables
    nt = state["validation"].get("new_tables", {})
    nt_status = nt.get("status", "unknown")
    if nt_status == "none_found":
        print("✅  New table discovery : No new tables")
    elif nt_status == "generated":
        count = nt.get("count", 0)
        print(f"🆕  New table discovery : {count} new model(s) generated")
        for t in nt.get("tables", []):
            print(f"    → {t['model']}.sql  (source: {t['table']})")
        print("    → Run dbt again to materialize new models")
    elif nt_status == "error":
        print(f"⚠️   New table discovery : {nt.get('message')}")

    # Collect all findings for LLM to summarise
    findings = []
    if f_status == "stale":
        findings.append(f"Data is stale ({freshness.get('age_hours')}h old)")
    if a_status == "anomaly_detected":
        for a in anomalies.get("anomalies", []):
            findings.append(a)
    if nt_status == "generated":
        findings.append(f"{nt.get('count')} new table(s) discovered and modelled")

    if findings:
        print("\n🤖  LLM Executive Summary:")
        summary_prompt = (
            f"You are a data engineering AI agent. Summarise this pipeline run for a manager.\n\n"
            f"Pipeline: Postgres → Airbyte → MotherDuck → dbt\n"
            f"dbt status: PASSED\n"
            f"Issues found:\n" +
            "\n".join(f"  - {f}" for f in findings) +
            f"\n\nWrite a 2-sentence executive summary of what happened and what needs attention.\n"
            f"Use plain English, no technical jargon, no markdown."
        )
        summary = llm.invoke(summary_prompt).strip()
        print(f"    {summary}")

    print("\n" + "=" * 60)
    state["final_status"] = "success"
    return state



# ── Node 8b: New Table Auto-Discovery (Scenario 3) ───────────────────────────
def discover_new_tables(state: PipelineState) -> PipelineState:
    """
    Scenario 3 — New Table Auto-Discovery:
    Scan MotherDuck for tables that exist but have no dbt model yet.
    For each new table found, use the LLM to generate:
      - A starter dbt model SQL file
      - A schema.yml entry with column descriptions and basic tests
    Saves both files directly into the models/ directory.
    """
    print("\n🔭  Scanning for new tables in MotherDuck...")

    try:
        con = md_connect()

        # Scan SOURCE_SCHEMA only (raw Airbyte tables)
        # Never scan MARTS_SCHEMA — those are dbt outputs, not sources
        all_tables = con.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SOURCE_SCHEMA}'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        con.close()

        all_table_names = [row[0] for row in all_tables]
        print(f"   Raw source tables in MotherDuck ({SOURCE_SCHEMA}): {all_table_names}")

        # Find which tables do NOT have a staging dbt model yet
        # Scan ALL subfolders recursively — not just the top-level models/ dir
        existing_models = []
        for root, dirs, files in os.walk(DBT_MODELS_DIR):
            for f in files:
                if f.endswith(".sql"):
                    existing_models.append(f.replace(".sql", ""))

        print(f"   Existing dbt models found: {existing_models}")

        # Skip Airbyte metadata tables and tables that already have a staging model
        skip_prefixes = ("_airbyte",)
        new_tables = [
            t for t in all_table_names
            if f"stg_{t}" not in existing_models
            and not any(t.startswith(p) for p in skip_prefixes)
        ]

        if not new_tables:
            print("   ✅  No new tables found — all tables have dbt models")
            state["new_tables"] = []
            state["validation"]["new_tables"] = {"status": "none_found"}
            return state

        print(f"   🆕  New tables detected: {new_tables}")
        state["new_tables"] = new_tables
        generated = []

        for table in new_tables:
            print(f"\n   📋  Generating dbt model for table: {table}")

            # Get schema of the new table
            con2 = md_connect()
            describe = con2.execute(f"DESCRIBE {DBT_SCHEMA}.{table}").fetchall()
            sample   = con2.execute(f"SELECT * FROM {DBT_SCHEMA}.{table} LIMIT 3").fetchall()
            con2.close()

            col_info   = [{"name": row[0], "type": row[1]} for row in describe]
            col_names  = [c["name"] for c in col_info if not c["name"].startswith("_airbyte")]
            sample_rows = ""
            for row in sample:
                d = dict(zip([c["name"] for c in col_info], row))
                sample_rows += "  " + ", ".join([f"{k}={v}" for k, v in d.items()
                                                  if not k.startswith("_airbyte")]) + "\n"

            schema_info = "\n".join([
                f"  - {c['name']} ({c['type']})"
                for c in col_info if not c["name"].startswith("_airbyte")
            ])

            # ── Ask LLM to generate the staging SQL ──────────────────────────
            # Give clear directions — LLM handles the column logic
            sql_prompt = (
                f"You are a dbt data engineer. Generate a dbt staging SQL model for a table called '{table}'.\n\n"
                f"Table columns and types (from live MotherDuck query):\n{schema_info}\n\n"
                f"Sample data (3 rows):\n{sample_rows}\n\n"
                f"Rules you MUST follow:\n"
                f"1. Skip ALL columns that start with '_airbyte' — do not include them in SELECT\n"
                f"2. For string/text/varchar/json columns: wrap with COALESCE(CAST(col AS VARCHAR), 'NA')\n"
                f"3. For date/timestamp/datetime columns: wrap with COALESCE(col, DATE '9999-12-31')\n"
                f"4. For numeric/int/decimal/float columns: wrap with COALESCE(col, 0)\n"
                f"5. For boolean columns: wrap with COALESCE(col, FALSE)\n"
                f"6. Use {{{{ source('airbyte_source', '{table}') }}}} as the source — do not change this\n"
                f"7. First line must be: {{{{ config(materialized='view', schema='staging') }}}}\n"
                f"8. Add a comment line: -- Auto-generated by AI Pipeline Agent\n\n"
                f"Reply with ONLY the SQL. No explanation. No markdown fences."
            )

            print("   🤖  Asking LLM to generate staging SQL...")
            sql_content = llm.invoke(sql_prompt).strip()
            sql_content = sql_content.replace("```sql", "").replace("```", "").strip()
            print(f"   LLM generated SQL preview:\n{sql_content[:300]}")

            # ── Ask LLM to generate column descriptions for schema.yml ───────
            desc_prompt = (
                f"For a database table called '{table}' with these columns:\n{schema_info}\n\n"
                f"Write a one-line business description for each column.\n"
                f"Skip any column starting with '_airbyte'.\n"
                f"Reply as: column_name: description\n"
                f"One column per line. Plain text only."
            )
            col_descriptions_raw = llm.invoke(desc_prompt).strip()

            # Parse LLM descriptions into a dict
            col_descs = {}
            for line in col_descriptions_raw.splitlines():
                if ":" in line:
                    parts = line.split(":", 1)
                    col_descs[parts[0].strip()] = parts[1].strip()

            # Build schema.yml column entries using LLM descriptions
            yaml_cols = ""
            for col in col_info:
                cname = col["name"]
                if cname.startswith("_airbyte"):
                    continue
                desc = col_descs.get(cname, f"{cname} field")
                yaml_cols += f"""      - name: {cname}
        description: "{desc}"
"""

            # Model entry (under "models:" section)
            model_entry_content = f"""  - name: stg_{table}
    description: "Staging model for {table} — auto-generated by AI Pipeline Agent"
    columns:
{yaml_cols}"""

            # Source table entry (under "sources: -> tables:" section)
            source_yaml_cols = ""
            for col in col_info:
                cname = col["name"]
                if cname.startswith("_airbyte"):
                    continue
                desc = col_descs.get(cname, f"{cname} field")
                source_yaml_cols += f"""          - name: {cname}
            description: "{desc}"
"""

            source_entry_content = f"""      - name: {table}
        description: "Raw {table} table ingested via Airbyte"
        columns:
{source_yaml_cols}"""

            # Write staging SQL model into models/staging/ (matches existing structure)
            os.makedirs(STAGING_DIR, exist_ok=True)
            model_name = f"stg_{table}"
            sql_path   = os.path.join(STAGING_DIR, f"{model_name}.sql")

            try:
                # Write the SQL model
                with open(sql_path, "w") as f:
                    f.write(sql_content)
                print(f"   ✓ Created {sql_path}")

                # Update the SINGLE schema.yml — add to both sources AND models
                schema_path = os.path.join(DBT_MODELS_DIR, "schema.yml")

                with open(schema_path, "r") as f:
                    current_schema_yml = f.read()

                # Backup with date stamp: schema_DDMMYYYY.yml.bak
                date_stamp = datetime.now().strftime("%d%m%Y")
                backup_path = os.path.join(
                    DBT_MODELS_DIR, f"schema_{date_stamp}.yml.bak"
                )
                with open(backup_path, "w") as f:
                    f.write(current_schema_yml)
                print(f"   ✓ Backup saved: {backup_path}")

                updated_schema_yml = current_schema_yml

                # 1. Insert new table under "sources: -> tables:" (after the last existing table)
                #    We append right before the "\nmodels:" marker, inside the tables: block
                if "    tables:" in updated_schema_yml:
                    # Find the end of the tables: block (right before "\nmodels:")
                    if "\nmodels:" in updated_schema_yml:
                        sources_part, models_part = updated_schema_yml.split("\nmodels:", 1)
                        sources_part = sources_part.rstrip() + "\n" + source_entry_content
                        updated_schema_yml = sources_part + "\nmodels:" + models_part
                    else:
                        updated_schema_yml = updated_schema_yml.rstrip() + "\n" + source_entry_content

                # 2. Append new model under "models:" section (at the end of file)
                if "\nmodels:" in updated_schema_yml:
                    updated_schema_yml = updated_schema_yml.rstrip() + "\n\n" + model_entry_content + "\n"
                else:
                    updated_schema_yml = updated_schema_yml.rstrip() + "\n\nmodels:\n" + model_entry_content + "\n"

                with open(schema_path, "w") as f:
                    f.write(updated_schema_yml)

                print(f"   ✓ Added '{table}' to sources AND 'stg_{table}' to models in {schema_path}")

                generated.append({
                    "table": table,
                    "model": model_name,
                    "sql_file": sql_path,
                    "schema_file": schema_path,
                })

            except Exception as e:
                print(f"   ✗ Failed to write files for {table}: {e}")

        state["validation"]["new_tables"] = {
            "status": "generated",
            "count": len(generated),
            "tables": generated,
        }
        print(f"\n   ✅  Generated {len(generated)} new dbt model(s)")

    except Exception as e:
        print(f"   Could not scan for new tables: {e}")
        state["validation"]["new_tables"] = {"status": "error", "message": str(e)}

    return state


# ── Run History Log (#5) ──────────────────────────────────────────────────────
def write_run_log(final_state: dict, duration_seconds: float):
    """
    Append a structured record of this agent run to pipeline_runs.json.
    Each entry captures: timestamp, duration, status, what was healed/found,
    freshness and anomaly results, and any LLM actions taken.
    """
    validation  = final_state.get("validation", {})
    freshness   = validation.get("freshness", {})
    anomalies   = validation.get("anomalies", {})
    new_tables  = validation.get("new_tables", {})

    record = {
        "timestamp":       datetime.now().isoformat(),
        "duration_seconds": duration_seconds,
        "final_status":    final_state.get("final_status", "unknown"),
        "dbt_success":     final_state.get("dbt_success", False),
        "retry_count":     final_state.get("retry_count", 0),
        "self_heal": {
            "triggered":    validation.get("status") == "fixable",
            "bad_col":      validation.get("bad_col"),
            "correct_col":  validation.get("correct_col"),
            "confidence":   validation.get("confidence"),
            "model_fixed":  validation.get("model_name"),
        },
        "freshness": {
            "status":       freshness.get("status"),
            "age_hours":    freshness.get("age_hours"),
            "last_record":  freshness.get("last_record"),
        },
        "anomalies": {
            "status":       anomalies.get("status"),
            "anomalies":    anomalies.get("anomalies", []),
            "today_orders": anomalies.get("today_orders"),
            "today_revenue":anomalies.get("today_revenue"),
        },
        "new_tables": {
            "status":  new_tables.get("status"),
            "count":   new_tables.get("count", 0),
            "tables":  [t.get("table") for t in new_tables.get("tables", [])],
        },
        "escalation": {
            "triggered": final_state.get("final_status") == "escalated",
            "reason":    validation.get("reason") if final_state.get("final_status") == "escalated" else None,
        }
    }

    # Load existing log or start fresh
    log = []
    if os.path.exists(RUN_LOG_PATH):
        try:
            with open(RUN_LOG_PATH, "r") as f:
                log = json.load(f)
        except (json.JSONDecodeError, IOError):
            log = []

    log.append(record)

    with open(RUN_LOG_PATH, "w") as f:
        json.dump(log, f, indent=2, default=str)

    print(f"\n📝  Run logged to {RUN_LOG_PATH} ({len(log)} total runs)")


# ── Routing ───────────────────────────────────────────────────────────────────
def route_after_dbt(state: PipelineState) -> Literal["validate", "success"]:
    return "success" if state["dbt_success"] else "validate"

def route_after_validation(state: PipelineState) -> Literal["heal", "alert", "success"]:
    status = state["validation"]["status"]
    if status == "ok":
        return "success"
    elif status == "fixable" and state.get("retry_count", 0) < 3:
        return "heal"
    return "alert"


# ── Build Graph ───────────────────────────────────────────────────────────────
def build_agent():
    graph = StateGraph(PipelineState)

    graph.add_node("run_dbt",            run_dbt)
    graph.add_node("validate",           validate_data)
    graph.add_node("heal",               heal_pipeline)
    graph.add_node("alert",              alert_team)
    graph.add_node("success",            pipeline_success)
    graph.add_node("check_freshness",    check_freshness)
    graph.add_node("detect_anomalies",   detect_anomalies)
    graph.add_node("discover_new_tables",discover_new_tables)
    graph.add_node("final_report",       final_report)

    graph.set_entry_point("run_dbt")
    graph.add_conditional_edges("run_dbt",          route_after_dbt)
    graph.add_conditional_edges("validate",         route_after_validation)
    graph.add_edge("heal",             "run_dbt")
    graph.add_edge("alert",            END)
    graph.add_edge("success",          "check_freshness")
    graph.add_edge("check_freshness",  "detect_anomalies")
    graph.add_edge("detect_anomalies",    "discover_new_tables")
    graph.add_edge("discover_new_tables", "final_report")
    graph.add_edge("final_report",        END)

    return graph.compile()


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not os.getenv("MOTHERDUCK_TOKEN"):
        print("❌  Missing MOTHERDUCK_TOKEN")
        print("    export MOTHERDUCK_TOKEN=<your-token>")
        exit(1)

    agent = build_agent()
    initial_state: PipelineState = {
        "dbt_output":      "",
        "dbt_run_output":  "",
        "dbt_test_output": "",
        "dbt_success":     False,
        "validation":      {},
        "retry_count":     0,
        "final_status":    "",
        "new_tables":      [],
    }

    run_start = time.time()
    print("🚀  Starting dbt pipeline agent...\n")
    final_state = agent.invoke(initial_state)
    run_duration = round(time.time() - run_start, 1)
    print(f"\n🏁  Final status: {final_state['final_status']}  ({run_duration}s)")

    # ── Write run to history log ───────────────────────────────────────────────
    write_run_log(final_state, run_duration)
