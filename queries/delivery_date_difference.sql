-- TODO: Esta consulta devolverá una tabla con dos columnas: State y Delivery_Difference.
-- La primera contendrá las letras que identifican los 
-- estados, y la segunda mostrará la diferencia promedio entre la fecha estimada 
-- de entrega y la fecha en la que los productos fueron realmente entregados al 
-- cliente.
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. Puedes usar la función CAST para convertir un número a un entero.
-- 3. Puedes usar la función STRFTIME para convertir order_delivered_customer_date a una cadena, eliminando horas, minutos y segundos.
-- 4. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL

WITH delivered_orders AS (
    SELECT
        c.customer_state AS State,
        STRFTIME('%Y-%m-%d', o.order_estimated_delivery_date) AS Estimated_Delivery,
        STRFTIME('%Y-%m-%d', o.order_delivered_customer_date) AS Actual_Delivery
    FROM olist_orders o
    JOIN olist_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'  
      AND o.order_delivered_customer_date IS NOT NULL  
),
delivery_differences AS (
    SELECT
        State,
        (julianday(Estimated_Delivery) - julianday(Actual_Delivery)) AS Delivery_Difference
    FROM delivered_orders
)
SELECT
    State,
    CAST(AVG(Delivery_Difference) AS INTEGER) AS Delivery_Difference  
FROM delivery_differences
GROUP BY State
ORDER BY Delivery_Difference ASC;

