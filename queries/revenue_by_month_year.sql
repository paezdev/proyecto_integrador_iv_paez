-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes en inglés (ej. Jan, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

WITH months AS (
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
),
aggregated AS (
  SELECT 
    strftime('%m', o.order_delivered_customer_date) AS month_no,
    SUM(CASE 
        WHEN strftime('%Y', o.order_delivered_customer_date) = '2016' THEN op.payment_value 
        ELSE 0 
    END) AS Year2016,
    SUM(CASE 
        WHEN strftime('%Y', o.order_delivered_customer_date) = '2017' THEN op.payment_value 
        ELSE 0 
    END) AS Year2017,
    SUM(CASE 
        WHEN strftime('%Y', o.order_delivered_customer_date) = '2018' THEN op.payment_value 
        ELSE 0 
    END) AS Year2018
  FROM olist_orders o
  JOIN (
    SELECT DISTINCT order_id, SUM(payment_value) AS payment_value
    FROM olist_order_payments
    WHERE payment_type IN ('credit_card', 'boleto')  -- Filtrar por tipos de pago
    GROUP BY order_id
  ) op ON o.order_id = op.order_id
  WHERE o.order_status = 'delivered'  -- Solo pedidos entregados
    AND o.order_delivered_customer_date IS NOT NULL  -- Fechas de entrega no nulas
    AND strftime('%Y', o.order_delivered_customer_date) IN ('2016', '2017', '2018')  -- Fechas dentro del rango
  GROUP BY month_no
)
SELECT 
  m.month_no,
  m.month,
  COALESCE(a.Year2016, 0.0) AS Year2016,
  COALESCE(a.Year2017, 0.0) AS Year2017,
  COALESCE(a.Year2018, 0.0) AS Year2018
FROM months m
LEFT JOIN aggregated a ON m.month_no = a.month_no
ORDER BY m.month_no;
