"""
Página de Análisis de Ventas
Análisis detallado de ventas por período, tienda y categoría
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Agregar ruta para imports
root_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_path))

from streamlit_app.utils.db_connection import db
from streamlit_app.utils.queries import Queries
from streamlit_app.components.kpi_cards import kpi_row, highlight_metric
from streamlit_app.components.filters import create_sidebar_filters
from streamlit_app.components.charts import create_line_chart, create_bar_chart, create_heatmap

# Configuración de página
st.set_page_config(
    page_title="Ventas - Sakila Dashboard",
    page_icon="📊",
    layout="wide"
)

# Header
st.title("📊 Análisis de Ventas")
st.markdown("---")

# Sidebar con filtros
with st.sidebar:
    st.markdown("## 🎬 Sakila Dashboard")
    st.markdown("### Análisis de Ventas")
    st.markdown("---")
    
    filters = create_sidebar_filters(db)
    
    st.markdown("---")
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
        st.rerun()

# Cargar datos
with st.spinner("📊 Cargando datos de ventas..."):
    df_ventas_mensuales = db.execute_query(Queries.VENTAS_MENSUALES_TIENDA)
    df_ventas_periodo = db.execute_query(Queries.VENTAS_POR_PERIODO)
    df_ventas_tienda = db.execute_query(Queries.VENTAS_POR_TIENDA)
    df_ventas_categoria = db.execute_query(Queries.VENTAS_POR_CATEGORIA)

# Aplicar filtros si existen
if filters and 'categorias' in filters and filters['categorias']:
    df_ventas_categoria = df_ventas_categoria[
        df_ventas_categoria['nombre_categoria'].isin(filters['categorias'])
    ]

if filters and 'tiendas' in filters and filters['tiendas']:
    df_ventas_mensuales = df_ventas_mensuales[
        df_ventas_mensuales['nombre_tienda'].isin(filters['tiendas'])
    ]
    df_ventas_tienda = df_ventas_tienda[
        df_ventas_tienda['nombre_tienda'].isin(filters['tiendas'])
    ]

# KPIs Principales
st.markdown("## 📈 Métricas Clave")

if not df_ventas_tienda.empty:
    total_ventas = df_ventas_tienda['total_ventas'].sum()
    total_rentas = df_ventas_tienda['total_rentas'].sum()
    ticket_promedio = df_ventas_tienda['ticket_promedio'].mean()
    dias_promedio = df_ventas_tienda['dias_promedio'].mean()
    
    kpi_row([
        {"label": "💰 Total Ventas", "value": f"${total_ventas:,.2f}"},
        {"label": "📦 Total Rentas", "value": f"{int(total_rentas):,}"},
        {"label": "🎟️ Ticket Promedio", "value": f"${ticket_promedio:.2f}"},
        {"label": "⏱️ Días Promedio", "value": f"{dias_promedio:.1f} días"}
    ])

st.markdown("---")

# Tabs de análisis
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Tendencia Temporal", 
    "🏪 Por Tienda", 
    "📂 Por Categoría", 
    "📋 Detalle"
])

# TAB 1: Tendencia Temporal
with tab1:
    st.markdown("### 📈 Evolución de Ventas en el Tiempo")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if not df_ventas_mensuales.empty:
            # Preparar datos
            df_ventas_mensuales['periodo'] = (
                df_ventas_mensuales['anio'].astype(str) + '-' + 
                df_ventas_mensuales['mes'].astype(str).str.zfill(2)
            )
            
            # Gráfico de líneas
            fig_temporal = create_line_chart(
                df_ventas_mensuales,
                x='periodo',
                y='ingresos_totales',
                color='nombre_tienda',
                title='Evolución de Ventas Mensuales por Tienda'
            )
            st.plotly_chart(fig_temporal, use_container_width=True)
            
            # Gráfico de rentas
            fig_rentas = create_line_chart(
                df_ventas_mensuales,
                x='periodo',
                y='total_rentas',
                color='nombre_tienda',
                title='Evolución de Rentas Mensuales por Tienda'
            )
            st.plotly_chart(fig_rentas, use_container_width=True)
        else:
            st.warning("No hay datos disponibles")
    
    with col2:
        st.markdown("#### 📊 Estadísticas")
        
        if not df_ventas_periodo.empty:
            # Mejor mes
            mejor_mes = df_ventas_periodo.nlargest(1, 'total_ventas').iloc[0]
            highlight_metric(
                "🏆 Mejor Mes",
                f"{mejor_mes['mes_nombre']} {int(mejor_mes['anio'])}",
                f"${mejor_mes['total_ventas']:,.2f}",
                color="#4CAF50"
            )
            
            # Peor mes
            peor_mes = df_ventas_periodo.nsmallest(1, 'total_ventas').iloc[0]
            highlight_metric(
                "📉 Menor Mes",
                f"{peor_mes['mes_nombre']} {int(peor_mes['anio'])}",
                f"${peor_mes['total_ventas']:,.2f}",
                color="#FF6B6B"
            )
            
            # Promedio mensual
            promedio_mensual = df_ventas_periodo['total_ventas'].mean()
            highlight_metric(
                "📊 Promedio Mensual",
                f"${promedio_mensual:,.2f}",
                f"{len(df_ventas_periodo)} meses analizados"
            )

# TAB 2: Por Tienda
with tab2:
    st.markdown("### 🏪 Análisis por Tienda")
    
    if not df_ventas_tienda.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de barras - Ventas
            fig_tiendas_ventas = create_bar_chart(
                df_ventas_tienda.sort_values('total_ventas', ascending=True),
                x='total_ventas',
                y='nombre_tienda',
                color='total_ventas',
                title='💰 Total de Ventas por Tienda',
                orientation='h'
            )
            st.plotly_chart(fig_tiendas_ventas, use_container_width=True)
        
        with col2:
            # Gráfico de barras - Rentas
            fig_tiendas_rentas = create_bar_chart(
                df_ventas_tienda.sort_values('total_rentas', ascending=True),
                x='total_rentas',
                y='nombre_tienda',
                color='total_rentas',
                title='📦 Total de Rentas por Tienda',
                orientation='h'
            )
            st.plotly_chart(fig_tiendas_rentas, use_container_width=True)
        
        # Tabla comparativa
        st.markdown("#### 📋 Tabla Comparativa")
        df_tiendas_display = df_ventas_tienda.copy()
        df_tiendas_display['total_ventas'] = df_tiendas_display['total_ventas'].apply(lambda x: f"${x:,.2f}")
        df_tiendas_display['ticket_promedio'] = df_tiendas_display['ticket_promedio'].apply(lambda x: f"${x:.2f}")
        df_tiendas_display['dias_promedio'] = df_tiendas_display['dias_promedio'].apply(lambda x: f"{x:.1f}")
        
        st.dataframe(
            df_tiendas_display[['nombre_tienda', 'ciudad', 'pais', 'total_rentas', 'total_ventas', 'ticket_promedio', 'dias_promedio']],
            use_container_width=True,
            hide_index=True
        )

# TAB 3: Por Categoría
with tab3:
    st.markdown("### 📂 Análisis por Categoría")
    
    if not df_ventas_categoria.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de pastel
            from streamlit_app.components.charts import create_pie_chart
            fig_categoria_pie = create_pie_chart(
                df_ventas_categoria,
                values='revenue_total',
                names='nombre_categoria',
                title='💰 Distribución de Ventas por Categoría',
                hole=0.4
            )
            st.plotly_chart(fig_categoria_pie, use_container_width=True)
        
        with col2:
            # Top categorías
            st.markdown("#### 🏆 Top 10 Categorías")
            top_categorias = df_ventas_categoria.nlargest(10, 'revenue_total')
            
            fig_top_cat = create_bar_chart(
                top_categorias.sort_values('revenue_total', ascending=True),
                x='revenue_total',
                y='nombre_categoria',
                title='Top 10 Categorías por Revenue',
                orientation='h'
            )
            st.plotly_chart(fig_top_cat, use_container_width=True)
        
        # Tabla detallada
        st.markdown("#### 📊 Detalle por Categoría")
        df_cat_display = df_ventas_categoria.copy()
        df_cat_display['revenue_total'] = df_cat_display['revenue_total'].apply(lambda x: f"${x:,.2f}")
        df_cat_display['ticket_promedio'] = df_cat_display['ticket_promedio'].apply(lambda x: f"${x:.2f}")
        df_cat_display['dias_promedio'] = df_cat_display['dias_promedio'].apply(lambda x: f"{x:.1f}")
        
        st.dataframe(df_cat_display, use_container_width=True, hide_index=True)

# TAB 4: Detalle
with tab4:
    st.markdown("### 📋 Datos Detallados")
    
    # Selector de vista
    vista_detalle = st.selectbox(
        "Seleccionar vista:",
        ["Ventas Mensuales", "Ventas por Tienda", "Ventas por Categoría"]
    )
    
    if vista_detalle == "Ventas Mensuales":
        st.dataframe(df_ventas_mensuales, use_container_width=True, hide_index=True)
        
        # Botón de descarga
        csv = df_ventas_mensuales.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name="ventas_mensuales.csv",
            mime="text/csv"
        )
    
    elif vista_detalle == "Ventas por Tienda":
        st.dataframe(df_ventas_tienda, use_container_width=True, hide_index=True)
        
        csv = df_ventas_tienda.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name="ventas_tienda.csv",
            mime="text/csv"
        )
    
    else:
        st.dataframe(df_ventas_categoria, use_container_width=True, hide_index=True)
        
        csv = df_ventas_categoria.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name="ventas_categoria.csv",
            mime="text/csv"
        )

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888;'>
        <p>📊 Análisis de Ventas | Sakila Data Warehouse</p>
    </div>
""", unsafe_allow_html=True)