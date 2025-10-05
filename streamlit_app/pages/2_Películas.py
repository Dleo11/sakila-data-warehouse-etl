"""
Página de Análisis de Películas
Análisis detallado de películas, clasificaciones y performance
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
from streamlit_app.components.filters import search_box
from streamlit_app.components.charts import create_bar_chart, create_scatter_chart, create_pie_chart

# Configuración de página
st.set_page_config(
    page_title="Películas - Sakila Dashboard",
    page_icon="🎬",
    layout="wide"
)

# Header
st.title("🎬 Análisis de Películas")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("## 🎬 Sakila Dashboard")
    st.markdown("### Análisis de Películas")
    st.markdown("---")
    
    # Filtros específicos de películas
    st.markdown("### 🔍 Filtros")
    
    # Buscar película
    buscar = search_box("Buscar película", "Ej: ACADEMY DINOSAUR")
    
    # Filtro de clasificación
    clasificaciones_disponibles = ['Todas', 'G', 'PG', 'PG-13', 'R', 'NC-17']
    clasificacion_filter = st.multiselect(
        "Clasificación",
        options=clasificaciones_disponibles,
        default=['Todas']
    )
    
    # Filtro de duración
    st.markdown("#### ⏱️ Duración (minutos)")
    duracion_min = st.slider("Duración mínima", 0, 200, 0)
    duracion_max = st.slider("Duración máxima", 0, 200, 200)
    
    st.markdown("---")
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
        st.rerun()

# Cargar datos
with st.spinner("🎬 Cargando datos de películas..."):
    df_top_peliculas = db.execute_query(Queries.TOP_PELICULAS)
    df_clasificacion = db.execute_query(Queries.PELICULAS_POR_CLASIFICACION)
    df_correlacion = db.execute_query(Queries.CORRELACION_DURACION_RENTAS)

# Aplicar filtros de búsqueda
if buscar:
    df_top_peliculas = df_top_peliculas[
        df_top_peliculas['titulo'].str.contains(buscar.upper(), na=False)
    ]

# Aplicar filtro de clasificación
if 'Todas' not in clasificacion_filter and clasificacion_filter:
    df_top_peliculas = df_top_peliculas[
        df_top_peliculas['clasificacion'].isin(clasificacion_filter)
    ]
    df_correlacion = df_correlacion[
        df_correlacion['clasificacion'].isin(clasificacion_filter)
    ]

# Aplicar filtro de duración
if not df_top_peliculas.empty:
    df_top_peliculas = df_top_peliculas[
        (df_top_peliculas['duracion'] >= duracion_min) & 
        (df_top_peliculas['duracion'] <= duracion_max)
    ]

# KPIs Principales
st.markdown("## 📊 Métricas Clave")

if not df_top_peliculas.empty:
    total_peliculas = len(df_top_peliculas)
    total_rentas = df_top_peliculas['total_rentas'].sum()
    revenue_total = df_top_peliculas['revenue_total'].sum()
    duracion_promedio = df_top_peliculas['duracion'].mean()
    tarifa_promedio = df_top_peliculas['tarifa_renta'].mean()
    
    kpi_row([
        {"label": "🎬 Total Películas", "value": f"{total_peliculas:,}"},
        {"label": "📦 Total Rentas", "value": f"{int(total_rentas):,}"},
        {"label": "💰 Revenue Total", "value": f"${revenue_total:,.2f}"},
        {"label": "⏱️ Duración Prom.", "value": f"{duracion_promedio:.0f} min"},
        {"label": "💵 Tarifa Prom.", "value": f"${tarifa_promedio:.2f}"}
    ])

st.markdown("---")

# Tabs de análisis
tab1, tab2, tab3, tab4 = st.tabs([
    "🏆 Top Películas", 
    "📊 Por Clasificación", 
    "🔬 Análisis Avanzado",
    "🔍 Búsqueda Detallada"
])

# TAB 1: Top Películas
with tab1:
    st.markdown("### 🏆 Películas Más Populares")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if not df_top_peliculas.empty:
            # Top 15 por rentas
            top_15 = df_top_peliculas.head(15)
            
            fig_top_rentas = create_bar_chart(
                top_15.sort_values('total_rentas', ascending=True),
                x='total_rentas',
                y='titulo',
                color='revenue_total',
                title='🏆 Top 15 Películas por Número de Rentas',
                orientation='h'
            )
            st.plotly_chart(fig_top_rentas, use_container_width=True)
            
            # Top 15 por revenue
            top_15_revenue = df_top_peliculas.nlargest(15, 'revenue_total')
            
            fig_top_revenue = create_bar_chart(
                top_15_revenue.sort_values('revenue_total', ascending=True),
                x='revenue_total',
                y='titulo',
                color='total_rentas',
                title='💰 Top 15 Películas por Revenue',
                orientation='h'
            )
            st.plotly_chart(fig_top_revenue, use_container_width=True)
    
    with col2:
        st.markdown("#### 🥇 Película #1")
        
        if not df_top_peliculas.empty:
            pelicula_top = df_top_peliculas.iloc[0]
            
            st.markdown(f"""
            <div style='
                background-color: #262730;
                padding: 20px;
                border-radius: 10px;
                border-left: 5px solid #FFD700;
            '>
                <h3 style='color: #FFD700; margin-top: 0;'>{pelicula_top['titulo']}</h3>
                <p style='color: #AAA; font-size: 14px;'>
                    <strong>Clasificación:</strong> {pelicula_top['clasificacion']}<br>
                    <strong>Duración:</strong> {int(pelicula_top['duracion'])} minutos<br>
                    <strong>Tarifa:</strong> ${pelicula_top['tarifa_renta']:.2f}<br>
                    <br>
                    <strong style='color: #4CAF50;'>Rentas:</strong> {int(pelicula_top['total_rentas']):,}<br>
                    <strong style='color: #4CAF50;'>Revenue:</strong> ${pelicula_top['revenue_total']:,.2f}<br>
                    <strong>Días Prom.:</strong> {pelicula_top['dias_promedio']:.1f} días
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Top 5 lista
            st.markdown("#### 🎯 Top 5")
            for idx, row in df_top_peliculas.head(5).iterrows():
                with st.expander(f"#{idx+1} - {row['titulo']}", expanded=False):
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.metric("📦 Rentas", f"{int(row['total_rentas']):,}")
                        st.metric("⏱️ Duración", f"{int(row['duracion'])} min")
                    with col_b:
                        st.metric("💰 Revenue", f"${row['revenue_total']:,.2f}")
                        st.metric("🎬 Rating", row['clasificacion'])

# TAB 2: Por Clasificación
with tab2:
    st.markdown("### 📊 Análisis por Clasificación")
    
    if not df_clasificacion.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Distribución por clasificación - Rentas
            fig_clasif_pie = create_pie_chart(
                df_clasificacion,
                values='total_rentas',
                names='clasificacion',
                title='📦 Distribución de Rentas por Clasificación',
                hole=0.4
            )
            st.plotly_chart(fig_clasif_pie, use_container_width=True)
            
            # Barras - Películas por clasificación
            fig_clasif_peliculas = create_bar_chart(
                df_clasificacion.sort_values('total_peliculas', ascending=False),
                x='clasificacion',
                y='total_peliculas',
                color='total_peliculas',
                title='🎬 Número de Películas por Clasificación'
            )
            st.plotly_chart(fig_clasif_peliculas, use_container_width=True)
        
        with col2:
            # Distribución por clasificación - Revenue
            fig_clasif_revenue = create_pie_chart(
                df_clasificacion,
                values='revenue_total',
                names='clasificacion',
                title='💰 Distribución de Revenue por Clasificación',
                hole=0.4
            )
            st.plotly_chart(fig_clasif_revenue, use_container_width=True)
            
            # Comparativa de métricas
            st.markdown("#### 📊 Comparativa de Métricas")
            
            import plotly.graph_objects as go
            
            fig_comparativa = go.Figure()
            
            fig_comparativa.add_trace(go.Bar(
                name='Duración Promedio',
                x=df_clasificacion['clasificacion'],
                y=df_clasificacion['duracion_promedio'],
                text=df_clasificacion['duracion_promedio'].round(0),
                textposition='auto'
            ))
            
            fig_comparativa.add_trace(go.Bar(
                name='Tarifa Promedio',
                x=df_clasificacion['clasificacion'],
                y=df_clasificacion['tarifa_promedio'],
                text=df_clasificacion['tarifa_promedio'].round(2),
                textposition='auto',
                yaxis='y2'
            ))
            
            fig_comparativa.update_layout(
                title='Duración vs Tarifa por Clasificación',
                yaxis=dict(title='Duración (min)'),
                yaxis2=dict(title='Tarifa ($)', overlaying='y', side='right'),
                barmode='group',
                height=400
            )
            
            st.plotly_chart(fig_comparativa, use_container_width=True)
        
        # Tabla detallada
        st.markdown("#### 📋 Tabla Comparativa por Clasificación")
        df_clasif_display = df_clasificacion.copy()
        df_clasif_display['revenue_total'] = df_clasif_display['revenue_total'].apply(lambda x: f"${x:,.2f}")
        df_clasif_display['tarifa_promedio'] = df_clasif_display['tarifa_promedio'].apply(lambda x: f"${x:.2f}")
        df_clasif_display['duracion_promedio'] = df_clasif_display['duracion_promedio'].apply(lambda x: f"{x:.0f} min")
        
        st.dataframe(df_clasif_display, use_container_width=True, hide_index=True)

# TAB 3: Análisis Avanzado
with tab3:
    st.markdown("### 🔬 Análisis de Correlaciones")
    
    if not df_correlacion.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Scatter: Duración vs Rentas
            fig_scatter_duracion = create_scatter_chart(
                df_correlacion,
                x='duracion',
                y='total_rentas',
                color='clasificacion',
                size='revenue',
                title='⏱️ Duración vs Número de Rentas'
            )
            st.plotly_chart(fig_scatter_duracion, use_container_width=True)
            
            st.info("""
            **💡 Insight:**
            Este gráfico muestra la relación entre la duración de las películas 
            y su popularidad (número de rentas). El tamaño de las burbujas 
            representa el revenue generado.
            """)
        
        with col2:
            # Scatter: Tarifa vs Revenue
            fig_scatter_tarifa = create_scatter_chart(
                df_correlacion,
                x='tarifa_renta',
                y='revenue',
                color='clasificacion',
                size='total_rentas',
                title='💵 Tarifa de Renta vs Revenue'
            )
            st.plotly_chart(fig_scatter_tarifa, use_container_width=True)
            
            st.info("""
            **💡 Insight:**
            Este gráfico analiza cómo la tarifa de renta afecta el revenue total.
            El tamaño de las burbujas representa el número de rentas.
            """)
        
        # Análisis de rangos de duración
        st.markdown("#### 📊 Análisis por Rangos de Duración")
        
        # Crear rangos
        df_correlacion['rango_duracion'] = pd.cut(
            df_correlacion['duracion'],
            bins=[0, 90, 120, 150, 200],
            labels=['Corta (<90min)', 'Media (90-120min)', 'Larga (120-150min)', 'Muy Larga (>150min)']
        )
        
        df_rangos = df_correlacion.groupby('rango_duracion').agg({
            'total_rentas': 'sum',
            'revenue': 'sum',
            'duracion': 'count'
        }).reset_index()
        df_rangos.columns = ['Rango Duración', 'Total Rentas', 'Revenue Total', 'Cantidad Películas']
        
        fig_rangos = create_bar_chart(
            df_rangos,
            x='Rango Duración',
            y='Total Rentas',
            color='Revenue Total',
            title='Rentas por Rango de Duración'
        )
        st.plotly_chart(fig_rangos, use_container_width=True)

# TAB 4: Búsqueda Detallada
with tab4:
    st.markdown("### 🔍 Búsqueda y Filtrado Avanzado")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Tabla con todas las películas filtradas
        if not df_top_peliculas.empty:
            st.markdown(f"#### 📋 Resultados ({len(df_top_peliculas)} películas)")
            
            # Formatear datos para display
            df_display = df_top_peliculas.copy()
            df_display['revenue_total'] = df_display['revenue_total'].apply(lambda x: f"${x:,.2f}")
            df_display['tarifa_renta'] = df_display['tarifa_renta'].apply(lambda x: f"${x:.2f}")
            df_display['costo_reemplazo'] = df_display['costo_reemplazo'].apply(lambda x: f"${x:.2f}")
            df_display['dias_promedio'] = df_display['dias_promedio'].apply(lambda x: f"{x:.1f}")
            
            # Seleccionar columnas a mostrar
            columnas_mostrar = [
                'titulo', 'clasificacion', 'duracion', 'tarifa_renta',
                'total_rentas', 'revenue_total', 'dias_promedio'
            ]
            
            st.dataframe(
                df_display[columnas_mostrar],
                use_container_width=True,
                hide_index=True,
                height=500
            )
            
            # Botón de descarga
            csv = df_top_peliculas.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar CSV Completo",
                data=csv,
                file_name="peliculas_analisis.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ No se encontraron películas con los filtros aplicados")
    
    with col2:
        st.markdown("#### 📊 Resumen de Resultados")
        
        if not df_top_peliculas.empty:
            # Estadísticas del conjunto filtrado
            st.metric("🎬 Películas", len(df_top_peliculas))
            st.metric("📦 Total Rentas", f"{int(df_top_peliculas['total_rentas'].sum()):,}")
            st.metric("💰 Revenue", f"${df_top_peliculas['revenue_total'].sum():,.2f}")
            
            st.markdown("---")
            
            # Distribución por clasificación en resultados
            clasif_dist = df_top_peliculas['clasificacion'].value_counts()
            st.markdown("**Distribución por Rating:**")
            for clasif, count in clasif_dist.items():
                st.write(f"**{clasif}:** {count} películas")

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888;'>
        <p>🎬 Análisis de Películas | Sakila Data Warehouse</p>
    </div>
""", unsafe_allow_html=True)