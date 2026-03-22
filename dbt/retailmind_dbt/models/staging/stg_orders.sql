-- This model cleans the raw data without deleting bad rows.
-- We FLAG issues instead of dropping — so the quality agent can find them.

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        UPPER(TRIM(status))               AS status,
        COALESCE(product_sku, 'UNKNOWN')  AS product_sku,
        order_date::DATE                  AS order_date,
        revenue,
        UPPER(TRIM(region))               AS region,

        -- Data quality flags (don't delete bad rows — flag them)
        CASE
            WHEN revenue < 0        THEN 'negative_revenue'
            WHEN revenue = 0        THEN 'zero_revenue'
            WHEN product_sku IS NULL THEN 'missing_sku'
            WHEN region IS NULL     THEN 'missing_region'
            ELSE 'ok'
        END AS dq_flag,

        _loaded_at
    FROM deduped
    WHERE rn = 1  -- remove duplicates, keep latest
)

SELECT * FROM cleaned