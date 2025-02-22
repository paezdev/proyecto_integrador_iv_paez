-- TODO: Esta consulta devolverá una tabla con dos columnas; customer_state y Revenue.
-- La primera contendrá las abreviaturas que identifican a los 10 estados con mayores ingresos,
-- y la segunda mostrará el ingreso total de cada uno.
-- PISTA: Todos los pedidos deben tener un estado "delivered" y la fecha real de entrega no debe ser nula.

WITH delivered_orders AS (
    SELECT 
        c.customer_state,
        (oi.price + oi.freight_value) AS total_revenue
    FROM olist_orders o
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    JOIN olist_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
)
SELECT 
    customer_state,
    SUM(total_revenue) AS Revenue  -- SQLite maneja los decimales automáticamente
FROM delivered_orders
GROUP BY customer_state
ORDER BY Revenue DESC
LIMIT 10;
