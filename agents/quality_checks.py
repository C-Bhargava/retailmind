# Each check returns a single number. The agent interprets what that number means.

CHECKS = {
    "duplicate_orders": {
        "sql": """
            SELECT COUNT(*) - COUNT(DISTINCT order_id) AS result
            FROM RETAILMIND.RAW.ORDERS
        """,
        "description": "Number of duplicate order_id rows in raw table",
        "threshold": 0,
        "severity": "high"
    },
    "negative_revenue": {
        "sql": """
            SELECT COUNT(*) AS result
            FROM RETAILMIND.STAGING.STG_ORDERS
            WHERE revenue < 0
        """,
        "description": "Orders with negative revenue values",
        "threshold": 0,
        "severity": "high"
    },
    "null_regions": {
        "sql": """
            SELECT COUNT(*) AS result
            FROM RETAILMIND.STAGING.STG_ORDERS
            WHERE region IS NULL
        """,
        "description": "Orders missing a region value",
        "threshold": 100,
        "severity": "medium"
    },
    "status_variants": {
        "sql": """
            SELECT COUNT(DISTINCT status) AS result
            FROM RETAILMIND.RAW.ORDERS
        """,
        "description": "Number of distinct status values (should be 2: COMPLETED, PENDING)",
        "threshold": 2,
        "severity": "medium"
    },
    "zero_revenue_rate": {
        "sql": """
            SELECT ROUND(
                COUNT(CASE WHEN revenue = 0 THEN 1 END) * 100.0 / COUNT(*), 2
            ) AS result
            FROM RETAILMIND.STAGING.STG_ORDERS
        """,
        "description": "Percentage of orders with zero revenue",
        "threshold": 1.0,
        "severity": "low"
    }
}