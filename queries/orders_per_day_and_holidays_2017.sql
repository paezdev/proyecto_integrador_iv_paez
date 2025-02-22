WITH daily_orders AS (
    SELECT 
        DATE(order_purchase_timestamp) AS order_date,  -- Alias para evitar conflictos
        COUNT(order_id) AS order_count
    FROM olist_orders
    WHERE order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
      AND order_purchase_timestamp IS NOT NULL
    GROUP BY order_date
)
SELECT 
    order_count,  -- Primera columna
    CAST(strftime('%s', order_date) AS INTEGER) * 1000 AS date,  -- Segunda columna
    EXISTS (  -- Tercera columna
        SELECT 1 
        FROM public_holidays 
        WHERE countryCode = 'BR' 
          AND DATE(date) = order_date  -- Comparación corregida
    ) AS holiday
FROM daily_orders
ORDER BY order_date;