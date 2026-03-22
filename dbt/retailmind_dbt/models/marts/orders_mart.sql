SELECT
    order_id,
    customer_id,
    product_sku,
    order_date,
    revenue,
    region,
    status,
    DATE_TRUNC('month', order_date) AS order_month,
    DATE_TRUNC('week',  order_date) AS order_week,
    DAYOFWEEK(order_date)           AS day_of_week
FROM {{ ref('stg_orders') }}
WHERE dq_flag = 'ok'
