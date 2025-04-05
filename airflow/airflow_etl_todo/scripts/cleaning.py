import sqlite3

def cleaning_data():
    db_path = '/opt/airflow/dags/airflow_etl_todo/data/ecommerce.db'

    tables = ['olist_orders', 'olist_order_payments', 'olist_customers']

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            for table in tables:
                silver_table = f"silver_{table}"
                bronze_table = f"bronze_{table}"

                print(f"⚙️ Procesando {bronze_table} → {silver_table}...")

                # Elimina la tabla Silver si ya existe
                cursor.execute(f"DROP TABLE IF EXISTS {silver_table}")

                # Crea la nueva tabla Silver copiando los datos de Bronze
                cursor.execute(f"CREATE TABLE {silver_table} AS SELECT * FROM {bronze_table}")

                print(f"✅ {silver_table} creada exitosamente.")

            conn.commit()
            print("✅ Limpieza completada, todas las tablas Silver están listas.")

    except Exception as e:
        print(f"❌ Error en cleaning_data: {e}")
        raise  # Para que Airflow registre el error
