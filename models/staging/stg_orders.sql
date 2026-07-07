-- Staging model: clean and rename raw Airbyte orders
-- Materialized as view in my_db.staging schema
{{ config(materialized='view', schema='staging') }}

SELECT
    id              AS order_id,
    customer_name,
    order_value,
    discount_pct,
    created_at
FROM {{ source('airbyte_source', 'orders') }}
WHERE id IS NOT NULL