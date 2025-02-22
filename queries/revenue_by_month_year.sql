-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes en inglés (ej. Jan, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

WITH revenue_data AS (
    SELECT 
        strftime('%Y', o.order_purchase_timestamp) AS year,
        strftime('%m', o.order_purchase_timestamp) AS month_no,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM olist_order_items oi
    JOIN olist_orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'  
      AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY year, month_no
),
month_names AS (
    SELECT '01' AS month_no, 'Jan' AS month UNION ALL
    SELECT '02', 'Feb' UNION ALL
    SELECT '03', 'Mar' UNION ALL
    SELECT '04', 'Apr' UNION ALL
    SELECT '05', 'May' UNION ALL
    SELECT '06', 'Jun' UNION ALL
    SELECT '07', 'Jul' UNION ALL
    SELECT '08', 'Aug' UNION ALL
    SELECT '09', 'Sep' UNION ALL
    SELECT '10', 'Oct' UNION ALL
    SELECT '11', 'Nov' UNION ALL
    SELECT '12', 'Dec'
)
SELECT 
    mn.month_no,
    mn.month,
    COALESCE(SUM(CASE WHEN rd.year = '2016' THEN rd.total_revenue END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN rd.year = '2017' THEN rd.total_revenue END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN rd.year = '2018' THEN rd.total_revenue END), 0.00) AS Year2018
FROM month_names mn
LEFT JOIN revenue_data rd ON mn.month_no = rd.month_no
GROUP BY mn.month_no, mn.month
ORDER BY mn.month_no;
