# agents/quality_agent.py
import os, json
from decimal import Decimal
import snowflake.connector
import anthropic
from dotenv import load_dotenv
from agents.quality_checks import CHECKS

load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE")
    )

def run_checks(conn):
    results = {}
    cur = conn.cursor()
    for name, check in CHECKS.items():
        cur.execute(check["sql"])
        value = float(cur.fetchone()[0])
        results[name] = {
            "value": value,
            "threshold": check["threshold"],
            "description": check["description"],
            "severity": check["severity"],
            "breached": value > check["threshold"]
        }
        status = "FAIL" if value > check["threshold"] else "PASS"
        print(f"  [{status}] {name}: {value}")
    return results

def diagnose_with_claude(results: dict) -> str:
    breached = {k: v for k, v in results.items() if v["breached"]}

    if not breached:
        return json.dumps({"status": "all_clear", "message": "All quality checks passed."})

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    prompt = f"""You are a senior data engineer reviewing automated quality checks
on a retail e-commerce data warehouse in Snowflake.

The following checks have BREACHED their thresholds:
{json.dumps(breached, indent=2)}

For each breached check, provide:
1. root_cause: Most likely reason this happened in a real pipeline
2. business_impact: What business decisions could be wrong because of this
3. fix_sql: The exact Snowflake SQL to fix or quarantine the bad rows
4. prevention: One dbt test to add to prevent this in future

Respond ONLY as a JSON array — no extra text, no markdown fences.
Format:
[
  {{
    "check": "check_name",
    "root_cause": "...",
    "business_impact": "...",
    "fix_sql": "...",
    "prevention": "..."
  }}
]"""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def log_results(conn, check_results: dict, recommendations: str):
    serializable = json.loads(
        json.dumps(check_results, default=lambda o: float(o) if isinstance(o, Decimal) else str(o))
    )
    check_json = json.dumps(serializable)
    rec_json = recommendations

    conn.cursor().execute(
        "INSERT INTO RETAILMIND.AGENTS.DQ_LOG (check_results, recommendations) "
        "SELECT PARSE_JSON(%s), PARSE_JSON(%s)",
        (check_json, rec_json)
    )
    print("  Results saved to AGENTS.DQ_LOG")

if __name__ == "__main__":
    print("\n=== RetailMind Quality Agent ===")
    conn = get_snowflake_conn()

    print("\nRunning quality checks...")
    results = run_checks(conn)

    breached_count = sum(1 for v in results.values() if v["breached"])
    print(f"\n{breached_count} check(s) breached threshold.")

    if breached_count > 0:
        print("\nCalling Claude for diagnosis...")
        diagnosis = diagnose_with_claude(results)
        print("\nClaude diagnosis:")
        parsed = json.loads(diagnosis)
        for item in parsed:
            print(f"\n  Issue: {item['check']}")
            print(f"  Root cause: {item['root_cause']}")
            print(f"  Business impact: {item['business_impact']}")
            print(f"  Fix SQL:\n    {item['fix_sql']}")
            print(f"  Prevention: {item['prevention']}")
    else:
        diagnosis = json.dumps({"status": "all_clear"})

    log_results(conn, results, diagnosis)
    conn.close()
    print("\nDone.")