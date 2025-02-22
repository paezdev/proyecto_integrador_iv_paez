-- TODO: Esta consulta devolverá una tabla con las 10 categorías con menores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con menores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.

WITH filtered_orders AS (
    SELECT
        oi.order_id,
        t.product_category_name_english AS category,  -- Traducción automática
        oi.price,
        oi.freight_value
    FROM olist_order_items oi
    JOIN olist_orders o ON oi.order_id = o.order_id
    JOIN olist_products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name  -- Traducción
    WHERE o.order_status = 'delivered'
      AND t.product_category_name_english IS NOT NULL
      AND o.order_delivered_customer_date IS NOT NULL
),
category_revenue AS (
    SELECT
        category,
        COUNT(DISTINCT order_id) AS num_order,  -- Contamos pedidos únicos
        SUM(price + freight_value) AS revenue  -- SUMAMOS DIRECTAMENTE price + freight_value por producto
    FROM filtered_orders
    GROUP BY category  
)
SELECT
    category AS Category,  
    num_order AS Num_order,  
    revenue AS Revenue  
FROM category_revenue
ORDER BY Revenue ASC  
LIMIT 10;
