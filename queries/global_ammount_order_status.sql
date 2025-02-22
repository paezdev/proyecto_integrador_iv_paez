-- TODO: Esta consulta devolverá una tabla con dos columnas: estado_pedido y
-- Cantidad. La primera contendrá las diferentes clases de estado de los pedidos,
-- y la segunda mostrará el total de cada uno.

SELECT 
    order_status AS order_status,  
    COUNT(*) AS Ammount  
FROM olist_orders  
GROUP BY order_status  
ORDER BY 
    CASE order_status  
        WHEN 'approved' THEN 1  
        WHEN 'canceled' THEN 2  
        WHEN 'created' THEN 3  
        WHEN 'delivered' THEN 4  
        WHEN 'invoiced' THEN 5  
        WHEN 'processing' THEN 6  
        WHEN 'shipped' THEN 7  
        WHEN 'unavailable' THEN 8  
        ELSE 9  
    END;
