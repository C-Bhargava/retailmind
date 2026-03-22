# app/dashboard.py
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="RetailMind",
    page_icon="assets/icon.png" if os.path.exists("assets/icon.png") else None,
    layout="wide"
)

st.title("RetailMind — Agentic Data Intelligence")
st.caption("Powered by Snowflake Cortex + Claude AI")

# ── Snowflake connection (cached) ──────────────────────────────────
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="RETAILMIND"
    )

def run_query(sql: str) -> pd.DataFrame:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

# ── Tabs ────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "Ask your data",
    "Data quality monitor",
    "Agent run history"
])

# ═══════════════════════════════════════════════════════════════════
# TAB 1 — Analyst agent
# ═══════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Ask a business question")
    st.write("Type any question about your retail data in plain English.")

    # Quick-pick example questions
    st.caption("Try one of these:")
    examples = [
        "What is total revenue by region?",
        "Which SKU has the most orders?",
        "What is the average order value by month?",
        "How many pending orders are there by region?"
    ]
    cols = st.columns(len(examples))
    for col, ex in zip(cols, examples):
        if col.button(ex, use_container_width=True):
            st.session_state["question"] = ex

    question = st.text_input(
        "Your question",
        value=st.session_state.get("question", ""),
        placeholder="e.g. What was total revenue last month by region?"
    )

    if st.button("Ask agent", type="primary") and question:
        with st.spinner("Agent thinking..."):
            from agents.analyst_agent import ask_cortex_analyst
            result = ask_cortex_analyst(question)

        st.success("Answer")
        st.write(result.get("answer", "No answer returned."))

        if result.get("sql"):
            with st.expander("Generated SQL", expanded=True):
                st.code(result["sql"], language="sql")

            with st.expander("Run the query and see results"):
                try:
                    df = run_query(result["sql"])
                    st.dataframe(df, use_container_width=True)

                    # Auto-chart if 2 columns returned
                    if len(df.columns) == 2:
                        num_cols = df.select_dtypes("number").columns
                        if len(num_cols) == 1:
                            cat_col = [c for c in df.columns if c not in num_cols][0]
                            st.bar_chart(df.set_index(cat_col))
                except Exception as e:
                    st.warning(f"Could not run query: {e}")

# ═══════════════════════════════════════════════════════════════════
# TAB 2 — Quality agent
# ═══════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Live data quality monitor")
    st.write("Run all 5 quality checks against your Snowflake tables.")

    if st.button("Run quality checks", type="primary"):
        from agents.quality_agent import get_snowflake_conn, run_checks, diagnose_with_claude, log_results

        with st.spinner("Running checks..."):
            conn = get_snowflake_conn()
            results = run_checks(conn)

        # Display metric cards
        st.divider()
        cols = st.columns(len(results))
        for col, (name, data) in zip(cols, results.items()):
            label = name.replace("_", " ").title()
            val = data["value"]
            breached = data["breached"]
            col.metric(
                label=label,
                value=f"{val:,.0f}",
                delta="FAIL" if breached else "PASS",
                delta_color="inverse" if breached else "normal"
            )

        breached_checks = {k: v for k, v in results.items() if v["breached"]}

        if breached_checks:
            st.divider()
            st.warning(f"{len(breached_checks)} check(s) need attention. Calling Claude for diagnosis...")

            with st.spinner("Claude diagnosing issues..."):
                import json
                diagnosis = diagnose_with_claude(results)
                log_results(conn, results, diagnosis)
                parsed = json.loads(diagnosis)

            st.success("Diagnosis complete — results saved to Snowflake")

            for item in parsed:
                with st.expander(f"Issue: {item['check'].replace('_',' ').title()}"):
                    st.markdown(f"**Root cause:** {item['root_cause']}")
                    st.markdown(f"**Business impact:** {item['business_impact']}")
                    st.code(item["fix_sql"], language="sql")
                    st.caption(f"Prevention: {item['prevention']}")
        else:
            st.success("All quality checks passed.")

        conn.close()

# ═══════════════════════════════════════════════════════════════════
# TAB 3 — Agent run history
# ═══════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Agent run history")
    st.write("Every quality agent run is logged to Snowflake.")

    if st.button("Load history"):
        try:
            df = run_query("""
                SELECT
                    RUN_ID,
                    TO_CHAR(RUN_TS, 'YYYY-MM-DD HH24:MI') AS run_time,
                    check_results:duplicate_orders:value::NUMBER   AS duplicate_orders,
                    check_results:negative_revenue:value::NUMBER   AS negative_revenue,
                    check_results:null_regions:value::NUMBER       AS null_regions,
                    check_results:status_variants:value::NUMBER    AS status_variants
                FROM RETAILMIND.AGENTS.DQ_LOG
                ORDER BY RUN_TS DESC
                LIMIT 20
            """)
            st.dataframe(df, use_container_width=True)

            if len(df) > 1:
                st.caption("Duplicate orders trend over runs:")
                st.line_chart(df.set_index("RUN_TIME")["DUPLICATE_ORDERS"])
        except Exception as e:
            st.error(f"Could not load history: {e}")