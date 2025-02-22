-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.

WITH delivered_orders AS (
    SELECT
        STRFTIME('%Y', o.order_purchase_timestamp) AS year,
        STRFTIME('%m', o.order_purchase_timestamp) AS month_no,
        (julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)) AS real_time,
        (julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp)) AS estimated_time
    FROM olist_orders o
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
),
aggregated_orders AS (
    SELECT
        month_no,
        CASE 
            WHEN month_no = '01' THEN 'Jan' WHEN month_no = '02' THEN 'Feb'
            WHEN month_no = '03' THEN 'Mar' WHEN month_no = '04' THEN 'Apr'
            WHEN month_no = '05' THEN 'May' WHEN month_no = '06' THEN 'Jun'
            WHEN month_no = '07' THEN 'Jul' WHEN month_no = '08' THEN 'Aug'
            WHEN month_no = '09' THEN 'Sep' WHEN month_no = '10' THEN 'Oct'
            WHEN month_no = '11' THEN 'Nov' ELSE 'Dec' 
        END AS month,
        AVG(CASE WHEN year = '2016' THEN real_time END) AS Year2016_real_time,
        AVG(CASE WHEN year = '2017' THEN real_time END) AS Year2017_real_time,
        AVG(CASE WHEN year = '2018' THEN real_time END) AS Year2018_real_time,
        AVG(CASE WHEN year = '2016' THEN estimated_time END) AS Year2016_estimated_time,
        AVG(CASE WHEN year = '2017' THEN estimated_time END) AS Year2017_estimated_time,
        AVG(CASE WHEN year = '2018' THEN estimated_time END) AS Year2018_estimated_time
    FROM delivered_orders
    GROUP BY month_no
)
SELECT 
    month_no,
    month,
    Year2016_real_time,
    Year2017_real_time,
    Year2018_real_time,
    Year2016_estimated_time,
    Year2017_estimated_time,
    Year2018_estimated_time
FROM aggregated_orders
ORDER BY month_no;