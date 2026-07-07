-- Mart model: daily order aggregation
-- Reads from staging, materializes as table in my_db.marts schema
{{ config(materialized='table', schema='marts') }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    DATE_TRUNC('day', created_at) AS order_date,
    COUNT(*)                      AS total_orders,
    SUM(order_value)              AS revenue
FROM staged
GROUP BY 1
ORDER BY 1 DESC