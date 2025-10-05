"""
Dashboard Principal - Sakila Data Warehouse
Análisis de Ventas de Películas
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Importar utilidades
from utils.db_connection import db
from utils.queries import Queries

# ========== CONFIGURACIÓN DE PÁGINA ==========
st.set_page_config(
    page_title="Sakila Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== ESTILOS CSS PERSONALIZADOS ==========
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #FAFAFA;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #262730;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #FF4B4B;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        background-color: #262730;
        border-radius: 5px;
    }
    </style>
""", unsafe_allow_html=True)

# ========== FUNCIÓN PARA CARGAR DATOS ==========
@st.cache_data(ttl=600)
def load_kpis():
    """Carga los KPIs principales"""
    return db.execute_query(Queries.KPI_TOTALES)

@st.cache_data(ttl=600)
def load_ventas_mensuales():
    """Carga ventas mensuales por tienda"""
    return db.execute_query(Queries.VENTAS_MENSUALES_TIENDA)

@st.cache_data(ttl=600)
def load_top_peliculas():
    """Carga top películas"""
    return db.execute_query(Queries.TOP_PELICULAS)

@st.cache_data(ttl=600)
def load_ventas_categoria():
    """Carga ventas por categoría"""
    return db.execute_query(Queries.VENTAS_POR_CATEGORIA)

@st.cache_data(ttl=600)
def load_performance_tiendas():
    """Carga performance por tienda"""
    return db.execute_query(Queries.VENTAS_POR_TIENDA)

# ========== SIDEBAR ==========
with st.sidebar:
    st.image("https://www.mysql.com/common/logos/logo-mysql-170x115.png", width=150)
    st.markdown("---")
    st.markdown("### 🎬 Sakila Dashboard")
    st.markdown("**Data Warehouse Analytics**")
    st.markdown("---")
    
    # Test de conexión
    if st.button("🔌 Test Conexión DB"):
        with st.spinner("Probando conexión..."):
            results = db.test_connection()
            for db_name, status in results.items():
                st.write(f"**{db_name}**: {status}")
    
    st.markdown("---")
    st.markdown("### 📊 Información del Sistema")
    st.info("""
    **ETL Pipeline**: ✅ Activo
    
    **Bases de Datos**:
    - Sakila (OLTP)
    - Sakila Staging
    - Sakila DW (OLAP)
    
    **Última actualización**: Ver logs
    """)
    
    st.markdown("---")
    st.markdown("### 👨‍💻 Desarrollado por")
    st.markdown("**Proyecto Académico**")
    st.markdown("Big Data & Analytics")

# ========== HEADER ==========
st.markdown('<h1 class="main-header">🎬 Sakila Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Sistema de Análisis de Ventas de Películas</p>', unsafe_allow_html=True)

# ========== CARGAR DATOS ==========
with st.spinner("📊 Cargando datos..."):
    try:
        df_kpis = load_kpis()
        df_ventas_mensuales = load_ventas_mensuales()
        df_top_peliculas = load_top_peliculas()
        df_categorias = load_ventas_categoria()
        df_tiendas = load_performance_tiendas()
        
        if df_kpis.empty:
            st.error("⚠️ No hay datos disponibles. Verifica que el ETL se haya ejecutado.")
            st.stop()
            
    except Exception as e:
        st.error(f"❌ Error al cargar datos: {str(e)}")
        st.stop()

# ========== KPIs PRINCIPALES ==========
st.markdown("## 📈 Indicadores Clave de Rendimiento")

# Extraer valores
total_rentas = int(df_kpis['total_rentas'].iloc[0])
total_ventas = float(df_kpis['total_ventas'].iloc[0])
ticket_promedio = float(df_kpis['ticket_promedio'].iloc[0])
dias_promedio = float(df_kpis['dias_promedio_renta'].iloc[0])
total_peliculas = int(df_kpis['total_peliculas_rentadas'].iloc[0])

# Crear 5 columnas para KPIs
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="💰 Total Ventas",
        value=f"${total_ventas:,.2f}",
        delta="Revenue Total"
    )

with col2:
    st.metric(
        label="📦 Total Rentas",
        value=f"{total_rentas:,}",
        delta="Transacciones"
    )

with col3:
    st.metric(
        label="🎟️ Ticket Promedio",
        value=f"${ticket_promedio:.2f}",
        delta="Por Renta"
    )

with col4:
    st.metric(
        label="⏱️ Días Promedio",
        value=f"{dias_promedio:.1f}",
        delta="Días de Renta"
    )

with col5:
    st.metric(
        label="🎬 Películas Rentadas",
        value=f"{total_peliculas:,}",
        delta="Títulos Únicos"
    )

st.markdown("---")

# ========== TABS PRINCIPALES ==========
tab1, tab2, tab3, tab4 = st.tabs(["📊 Ventas", "🎬 Películas", "📂 Categorías", "🏪 Tiendas"])

# ========== TAB 1: VENTAS ==========
with tab1:
    st.markdown("### 💵 Análisis de Ventas")
    
    # Gráfico de ventas mensuales
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Preparar datos para el gráfico
        if not df_ventas_mensuales.empty:
            # Crear columna de fecha para mejor visualización
            df_ventas_mensuales['periodo'] = df_ventas_mensuales['anio'].astype(str) + '-' + df_ventas_mensuales['mes'].astype(str).str.zfill(2)
            
            fig_ventas = px.line(
                df_ventas_mensuales,
                x='periodo',
                y='ingresos_totales',  # ✅ CORREGIDO: era 'total_ventas'
                color='nombre_tienda',
                title='📈 Evolución de Ventas Mensuales por Tienda',
                labels={'periodo': 'Período', 'ingresos_totales': 'Ventas ($)', 'nombre_tienda': 'Tienda'},
                markers=True
            )
            fig_ventas.update_layout(
                height=400,
                hovermode='x unified',
                showlegend=True
            )
            st.plotly_chart(fig_ventas, use_container_width=True)
        else:
            st.warning("No hay datos de ventas mensuales disponibles")
    
    with col2:
        # Top 5 meses con más ventas
        st.markdown("#### 🏆 Top 5 Meses")
        if not df_ventas_mensuales.empty:
            top_meses = df_ventas_mensuales.nlargest(5, 'ingresos_totales')[['periodo', 'nombre_tienda', 'ingresos_totales']]  # ✅ CORREGIDO
            for idx, row in top_meses.iterrows():
                st.metric(
                    label=f"{row['periodo']} - {row['nombre_tienda']}",
                    value=f"${row['ingresos_totales']:,.2f}"
                )

# ========== TAB 2: PELÍCULAS ==========
with tab2:
    st.markdown("### 🎬 Top Películas Más Rentadas")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if not df_top_peliculas.empty:
            # Gráfico de barras
            fig_peliculas = px.bar(
                df_top_peliculas.head(15),
                x='total_rentas',
                y='titulo',
                orientation='h',
                title='🏆 Top 15 Películas por Número de Rentas',
                labels={'total_rentas': 'Total Rentas', 'titulo': 'Película'},
                color='revenue_total',
                color_continuous_scale='Reds'
            )
            fig_peliculas.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig_peliculas, use_container_width=True)
        else:
            st.warning("No hay datos de películas disponibles")
    
    with col2:
        st.markdown("#### 📊 Estadísticas")
        if not df_top_peliculas.empty:
            pelicula_top = df_top_peliculas.iloc[0]
            st.info(f"""
            **🥇 Película #1**
            
            {pelicula_top['titulo']}
            
            **Rentas**: {int(pelicula_top['total_rentas']):,}
            
            **Revenue**: ${pelicula_top['revenue_total']:,.2f}
            
            **Clasificación**: {pelicula_top['clasificacion']}
            
            **Duración**: {int(pelicula_top['duracion'])} min
            """)

# ========== TAB 3: CATEGORÍAS ==========
with tab3:
    st.markdown("### 📂 Análisis por Categoría")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not df_categorias.empty:
            # Gráfico de pastel
            fig_categoria_pie = px.pie(
                df_categorias,
                values='revenue_total',
                names='nombre_categoria',
                title='💰 Distribución de Revenue por Categoría',
                hole=0.4
            )
            fig_categoria_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_categoria_pie, use_container_width=True)
    
    with col2:
        if not df_categorias.empty:
            # Gráfico de barras
            fig_categoria_bar = px.bar(
                df_categorias.sort_values('total_rentas', ascending=True),
                x='total_rentas',
                y='nombre_categoria',
                orientation='h',
                title='📦 Total de Rentas por Categoría',
                labels={'total_rentas': 'Total Rentas', 'nombre_categoria': 'Categoría'},
                color='total_rentas',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_categoria_bar, use_container_width=True)
    
    # Tabla detallada
    st.markdown("#### 📋 Detalle por Categoría")
    if not df_categorias.empty:
        df_categorias_display = df_categorias.copy()
        df_categorias_display['revenue_total'] = df_categorias_display['revenue_total'].apply(lambda x: f"${x:,.2f}")
        df_categorias_display['ticket_promedio'] = df_categorias_display['ticket_promedio'].apply(lambda x: f"${x:.2f}")
        df_categorias_display['dias_promedio'] = df_categorias_display['dias_promedio'].apply(lambda x: f"{x:.1f}")
        st.dataframe(df_categorias_display, use_container_width=True, hide_index=True)

# ========== TAB 4: TIENDAS ==========
with tab4:
    st.markdown("### 🏪 Comparativa de Tiendas")
    
    if not df_tiendas.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Comparativa de ventas
            fig_tiendas = go.Figure()
            
            for idx, row in df_tiendas.iterrows():
                fig_tiendas.add_trace(go.Bar(
                    name=row['nombre_tienda'],
                    x=['Rentas', 'Ventas', 'Ticket Promedio'],
                    y=[row['total_rentas'], row['total_ventas'], row['ticket_promedio']],
                    text=[f"{row['total_rentas']:,.0f}", f"${row['total_ventas']:,.2f}", f"${row['ticket_promedio']:.2f}"],
                    textposition='auto'
                ))
            
            fig_tiendas.update_layout(
                title='📊 Comparativa de Métricas por Tienda',
                barmode='group',
                height=400
            )
            st.plotly_chart(fig_tiendas, use_container_width=True)
        
        with col2:
            st.markdown("#### 📍 Información de Tiendas")
            for idx, row in df_tiendas.iterrows():
                with st.expander(f"🏪 {row['nombre_tienda']}", expanded=True):
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.metric("📦 Rentas", f"{int(row['total_rentas']):,}")
                        st.metric("💰 Ventas", f"${row['total_ventas']:,.2f}")
                    with col_b:
                        st.metric("🎟️ Ticket Prom.", f"${row['ticket_promedio']:.2f}")
                        st.metric("⏱️ Días Prom.", f"{row['dias_promedio']:.1f}")
                    st.info(f"📍 **Ubicación**: {row['ciudad']}, {row['pais']}")

# ========== FOOTER ==========
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888;'>
        <p>📊 Sakila Data Warehouse Dashboard | Desarrollado con Streamlit + Python + MySQL</p>
        <p>🔄 Datos actualizados mediante proceso ETL | Sistema OLAP</p>
    </div>
""", unsafe_allow_html=True)