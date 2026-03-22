# agents/analyst_agent.py
import os, json, requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SEMANTIC_MODEL_PATH = "@RETAILMIND.AGENTS.SEMANTIC_STAGE/semantic_model.yaml"

def ask_cortex_analyst(question: str) -> dict:
    """Send a natural language question to Cortex Analyst via SQL."""
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="RETAILMIND"
    )

    # Escape single quotes in the question
    safe_question = question.replace("'", "\\'")

    # Call Cortex Analyst directly via SQL function
    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
            CONCAT(
                'You are a SQL expert for a retail data warehouse. ',
                'Answer this question by writing a Snowflake SQL query ',
                'against the table RETAILMIND.MARTS.ORDERS_MART which has columns: ',
                'ORDER_ID, CUSTOMER_ID, PRODUCT_SKU, ORDER_DATE, REVENUE, REGION, ',
                'STATUS, ORDER_MONTH, ORDER_WEEK, DAY_OF_WEEK. ',
                'Question: {safe_question}. ',
                'Respond in JSON with keys: answer (plain English) and sql (the query). ',
                'Return ONLY the JSON, no markdown.'
            )
        ) AS response
    """

    cur = conn.cursor()
    cur.execute(sql)
    raw = cur.fetchone()[0]
    conn.close()

    # Parse the JSON response
    try:
        # Strip markdown fences if present
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[1:-1])
        result = json.loads(cleaned)
    except Exception:
        result = {"answer": raw, "sql": ""}

    return result

DEMO_QUESTIONS = [
    "What is the total revenue by region?",
    "Which product SKU has the highest order count?",
    "What is the average order value by month?",
    "How many unique customers placed orders each month?",
    "What percentage of orders are still pending by region?"
]

if __name__ == "__main__":
    print("\n=== RetailMind Analyst Agent ===\n")
    for question in DEMO_QUESTIONS:
        print(f"Q: {question}")
        try:
            result = ask_cortex_analyst(question)
            print(f"A: {result.get('answer', 'No answer')}")
            if result.get('sql'):
                print(f"SQL:\n{result['sql']}")
        except Exception as e:
            print(f"Error: {e}")
        print("-" * 60)