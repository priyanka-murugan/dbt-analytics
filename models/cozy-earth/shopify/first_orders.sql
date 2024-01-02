WITH ranked_orders AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY created_at ASC) AS order_num
    FROM
        shopify.order
)
SELECT *
FROM ranked_orders
WHERE order_num = 1
