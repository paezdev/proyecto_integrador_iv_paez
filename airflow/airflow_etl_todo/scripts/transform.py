import sqlite3
import os

def transform_data():
    db_path = '/opt/airflow/dags/airflow_etl_todo/data/ecommerce.db'

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            print("🚀 Ejecutando queries para crear tablas Gold...")

            # Query 1: Top 10 estados con mayor ingreso
            query1 = """
            DROP TABLE IF EXISTS gold_top_states;
            CREATE TABLE gold_top_states AS
            SELECT 
                c.customer_state AS state,
                SUM(p.payment_value) AS total_revenue
            FROM silver_olist_order_payments p
            JOIN silver_olist_orders o ON p.order_id = o.order_id
            JOIN silver_olist_customers c ON o.customer_id = c.customer_id
            GROUP BY c.customer_state
            ORDER BY total_revenue DESC
            LIMIT 10;
            """
            cursor.executescript(query1)
            print("✅ Tabla 'gold_top_states' creada.")

            # Query 2: Comparación de tiempos reales vs estimados por mes y año
            query2 = """
            DROP TABLE IF EXISTS gold_delivery_comparison;
            CREATE TABLE gold_delivery_comparison AS
            SELECT 
                strftime('%Y-%m', o.order_purchase_timestamp) AS month_year,
                AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)) AS actual_days,
                AVG(julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp)) AS estimated_days
            FROM silver_olist_orders o
            WHERE o.order_delivered_customer_date IS NOT NULL
            GROUP BY month_year
            ORDER BY month_year;
            """
            cursor.executescript(query2)
            print("✅ Tabla 'gold_delivery_comparison' creada.")

            conn.commit()
            print("✅ Transformación completada. Tablas Gold listas en ecommerce.db.")

    except Exception as e:
        print(f"❌ Error en transform_data: {e}")
        raise  # Para que Airflow registre el error

