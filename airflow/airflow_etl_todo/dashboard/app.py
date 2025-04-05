import streamlit as st
import pandas as pd
import sqlite3
import altair as alt
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Ruta a la base de datos
DB_PATH = 'data/ecommerce.db'

st.set_page_config(page_title="Análisis de Pedidos", layout="wide")

st.title("📦 Análisis de Pedidos - Olist")
st.markdown("""
Este dashboard presenta un análisis de rendimiento logístico y comercial a partir de los datos de Olist.  
Las visualizaciones responden a preguntas clave sobre ingresos generados y precisión en tiempos de entrega.
""")

# Verificar existencia de base de datos
if not os.path.exists(DB_PATH):
    st.error("❌ No se encontró la base de datos. Verifica la ruta.")
    st.stop()

# Conexión a SQLite
conn = sqlite3.connect('data/ecommerce.db')

# -------------------------------
# 🧠 Pregunta 1: Ingresos por estado
# -------------------------------

st.header("💰 Top 10 estados con mayor ingreso")

query1 = "SELECT * FROM gold_top_states"
df_states = pd.read_sql(query1, conn)

col1, col2 = st.columns([2, 1])

with col1:
    chart = alt.Chart(df_states).mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Ingresos (BRL)"),
        y=alt.Y('state:N', sort='-x', title="Estado"),
        tooltip=['state', 'total_revenue']
    ).properties(
        height=400,
        title="Top 10 estados con mayor ingreso generado por pedidos entregados"
    )
    st.altair_chart(chart, use_container_width=True)

with col2:
    st.dataframe(df_states, use_container_width=True)

# -------------------------------
# 🧠 Pregunta 2: Comparación de tiempos reales vs estimados
# -------------------------------

st.header("⏱️ Comparación mensual de tiempos de entrega reales vs. estimados")

query2 = "SELECT * FROM gold_delivery_comparison"
df_delivery = pd.read_sql(query2, conn)

# Reestructurar para gráfico: convertir actual vs estimado en formato largo
df_long = df_delivery.melt(
    id_vars=["month_year"],
    value_vars=["actual_days", "estimated_days"],
    var_name="time_type",
    value_name="delivery_time"
)

# Renombrar para visualización más clara
df_long["time_type"] = df_long["time_type"].map({
    "actual_days": "Real",
    "estimated_days": "Estimado"
})

# Gráfico de comparación
line_chart = alt.Chart(df_long).mark_line(point=True).encode(
    x=alt.X("month_year:N", title="Mes"),
    y=alt.Y("delivery_time:Q", title="Tiempo promedio de entrega (días)"),
    color=alt.Color("time_type:N", title="Tipo de tiempo"),
    tooltip=["month_year", "time_type", "delivery_time"]
).properties(
    height=400,
    title="Tiempos de entrega reales vs. estimados"
)

st.altair_chart(line_chart, use_container_width=True)


conn.close()

