# snowflake/upload_semantic_model.py
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="RETAILMIND"
)

# Upload the YAML file to the Snowflake stage
conn.cursor().execute("""
    PUT file://snowflake/semantic_model.yaml
    @RETAILMIND.AGENTS.SEMANTIC_STAGE
    OVERWRITE = TRUE
    AUTO_COMPRESS = FALSE
""")

print("Semantic model uploaded successfully.")

# Verify it's there
cur = conn.cursor()
cur.execute("LIST @RETAILMIND.AGENTS.SEMANTIC_STAGE")
for row in cur.fetchall():
    print(f"  Found: {row[0]}")

conn.close()