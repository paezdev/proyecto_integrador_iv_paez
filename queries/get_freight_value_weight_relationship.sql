SELECT 
    oi.order_id, 
    oi.freight_value, 
    p.product_weight_g
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id;