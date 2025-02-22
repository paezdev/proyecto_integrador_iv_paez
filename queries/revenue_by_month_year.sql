-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes en inglés (ej. Jan, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

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
    SUM(CASE WHEN strftime('%Y', o.order_delivered_customer_date) = '2016' THEN op.payment_value ELSE 0 END) AS Year2016,
    SUM(CASE WHEN strftime('%Y', o.order_delivered_customer_date) = '2017' THEN op.payment_value ELSE 0 END) AS Year2017,
    SUM(CASE WHEN strftime('%Y', o.order_delivered_customer_date) = '2018' THEN op.payment_value ELSE 0 END) AS Year2018
FROM olist_orders o
JOIN olist_order_payments op ON o.order_id = op.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
  AND op.payment_type IN ('credit_card', 'boleto')
GROUP BY strftime('%m', o.order_delivered_customer_date)
ORDER BY strftime('%m', o.order_delivered_customer_date);
