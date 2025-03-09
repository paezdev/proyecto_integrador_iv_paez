-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes en inglés (ej. Jan, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

WITH payment_counts AS (
    SELECT 
        o.order_id,
        COUNT(DISTINCT op.payment_sequential) as total_payments_per_order,
        COUNT(*) OVER (PARTITION BY o.order_id, op.payment_type) as duplicate_count
    FROM olist_orders o
    JOIN olist_order_payments op ON o.order_id = op.order_id
    WHERE op.payment_type IN ('credit_card', 'boleto', 'debit_card')
    GROUP BY o.order_id, op.payment_type
),
filtered_orders AS (
    SELECT 
        strftime('%m', o.order_delivered_customer_date) AS month_no,
        CASE strftime('%m', o.order_delivered_customer_date)
             WHEN '01' THEN 'Jan'
             WHEN '02' THEN 'Feb'
             WHEN '03' THEN 'Mar'
             WHEN '04' THEN 'Apr'
             WHEN '05' THEN 'May'
             WHEN '06' THEN 'Jun'
             WHEN '07' THEN 'Jul'
             WHEN '08' THEN 'Aug'
             WHEN '09' THEN 'Sep'
             WHEN '10' THEN 'Oct'
             WHEN '11' THEN 'Nov'
             WHEN '12' THEN 'Dec'
        END AS month,
        strftime('%Y', o.order_delivered_customer_date) AS year,
        op.payment_value
    FROM olist_orders o
    JOIN olist_order_payments op ON o.order_id = op.order_id
    JOIN payment_counts pc ON o.order_id = pc.order_id
    WHERE o.order_status = 'delivered'
        AND o.order_delivered_customer_date IS NOT NULL
        AND op.payment_sequential = 1
        AND op.payment_type IN ('credit_card', 'boleto', 'debit_card')
        AND pc.duplicate_count = 1
        AND pc.total_payments_per_order = 1
)
SELECT 
    month_no,
    month,
    SUM(CASE WHEN year = '2016' THEN payment_value ELSE 0 END) AS Year2016,
    SUM(CASE WHEN year = '2017' THEN payment_value ELSE 0 END) AS Year2017,
    SUM(CASE WHEN year = '2018' THEN payment_value ELSE 0 END) AS Year2018
FROM filtered_orders
GROUP BY month_no
ORDER BY month_no;
