{{
config(
  materialized='incremental',
  unique_key='id',
  tags = ["incremental_model"],
  )
}}

WITH ranked_orders AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY created_at ASC) AS order_num
    FROM
        shopify.order
)
SELECT *
FROM ranked_orders
WHERE order_num = 1

{% if is_incremental() %}

where created_at > (select max(created_at) from {{ this }})

{% endif %}