"""
Página de Análisis de Categorías
Análisis detallado por género de películas
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
from streamlit_app.components.charts import create_bar_chart, create_pie_chart, create_line_chart

# Configuración de página
st.set_page_config(
    page_title="Categorías - Sakila Dashboard",
    page_icon="📂",
    layout="wide"
)

# Header
st.title("📂 Análisis por Categorías")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("## 🎬 Sakila Dashboard")
    st.markdown("### Análisis de Categorías")
    st.markdown("---")
    
    st.markdown("### 🔍 Filtros")
    
    # Selector de métricas
    metrica_ordenar = st.selectbox(
        "Ordenar por:",
        ["Revenue Total", "Total Rentas", "Ticket Promedio", "Días Promedio"]
    )
    
    # Número de categorías a mostrar
    top_n = st.slider("Top N Categorías", 5, 16, 10)
    
    st.markdown("---")
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    st.info("""
    **💡 Sobre las Categorías:**
    
    Las categorías representan los géneros cinematográficos disponibles 
    en el catálogo de Sakila.
    
    Este análisis permite identificar qué géneros son más populares 
    y rentables.
    """)

# Cargar datos
with st.spinner("📂 Cargando datos de categorías..."):
    df_categorias = db.execute_query(Queries.VENTAS_POR_CATEGORIA)
    df_performance = db.execute_query(Queries.PERFORMANCE_CATEGORIA)
    df_tendencia = db.execute_query(Queries.CATEGORIA_TENDENCIA_TEMPORAL)
    df_top_films_cat = db.execute_query(Queries.TOP_FILMS_CATEGORIA)

# KPIs Principales
st.markdown("## 📊 Métricas Generales")

if not df_categorias.empty:
    total_categorias = len(df_categorias)
    total_peliculas = df_categorias['total_peliculas'].sum()
    total_rentas = df_categorias['total_rentas'].sum()
    revenue_total = df_categorias['revenue_total'].sum()
    ticket_promedio = df_categorias['ticket_promedio'].mean()
    
    kpi_row([
        {"label": "📂 Total Categorías", "value": f"{total_categorias}"},
        {"label": "🎬 Total Películas", "value": f"{int(total_peliculas):,}"},
        {"label": "📦 Total Rentas", "value": f"{int(total_rentas):,}"},
        {"label": "💰 Revenue Total", "value": f"${revenue_total:,.2f}"},
        {"label": "🎟️ Ticket Prom.", "value": f"${ticket_promedio:.2f}"}
    ])

st.markdown("---")

# Tabs de análisis
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Ranking", 
    "📈 Tendencias", 
    "🎬 Top Películas",
    "📋 Detalle"
])

# TAB 1: Ranking
with tab1:
    st.markdown("### 📊 Ranking de Categorías")
    
    if not df_categorias.empty:
        # Mapear selección a columna
        metrica_map = {
            "Revenue Total": "revenue_total",
            "Total Rentas": "total_rentas",
            "Ticket Promedio": "ticket_promedio",
            "Días Promedio": "dias_promedio"
        }
        
        columna_ordenar = metrica_map[metrica_ordenar]
        df_sorted = df_categorias.nlargest(top_n, columna_ordenar)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Gráfico principal
            fig_ranking = create_bar_chart(
                df_sorted.sort_values(columna_ordenar, ascending=True),
                x=columna_ordenar,
                y='nombre_categoria',
                color=columna_ordenar,
                title=f'Top {top_n} Categorías por {metrica_ordenar}',
                orientation='h'
            )
            st.plotly_chart(fig_ranking, use_container_width=True)
            
            # Gráfico de pastel - Distribución de Revenue
            fig_pie_revenue = create_pie_chart(
                df_sorted,
                values='revenue_total',
                names='nombre_categoria',
                title='💰 Distribución de Revenue',
                hole=0.4
            )
            st.plotly_chart(fig_pie_revenue, use_container_width=True)
        
        with col2:
            st.markdown("#### 🏆 Top 3 Categorías")
            
            top_3 = df_sorted.head(3)
            
            medallas = ["🥇", "🥈", "🥉"]
            colores = ["#FFD700", "#C0C0C0", "#CD7F32"]
            
            for idx, (i, row) in enumerate(top_3.iterrows()):
                highlight_metric(
                    f"{medallas[idx]} {row['nombre_categoria']}",
                    f"${row['revenue_total']:,.2f}",
                    f"{int(row['total_rentas']):,} rentas | {int(row['total_peliculas'])} películas",
                    color=colores[idx]
                )
            
            st.markdown("---")
            st.markdown("#### 📊 Comparativa Visual")
            
            # Mini tabla comparativa
            st.dataframe(
                df_sorted[['nombre_categoria', 'total_rentas', 'total_peliculas']].head(5),
                use_container_width=True,
                hide_index=True
            )

# TAB 2: Tendencias
with tab2:
    st.markdown("### 📈 Evolución Temporal por Categoría")
    
    if not df_tendencia.empty:
        # Selector de categorías para comparar
        categorias_disponibles = df_tendencia['nombre_categoria'].unique().tolist()
        categorias_seleccionadas = st.multiselect(
            "Seleccionar categorías para comparar:",
            options=categorias_disponibles,
            default=categorias_disponibles[:5] if len(categorias_disponibles) >= 5 else categorias_disponibles
        )
        
        if categorias_seleccionadas:
            df_filtrado = df_tendencia[
                df_tendencia['nombre_categoria'].isin(categorias_seleccionadas)
            ]
            
            # Crear columna de periodo
            df_filtrado['periodo'] = (
                df_filtrado['anio'].astype(str) + '-' + 
                df_filtrado['mes'].astype(str).str.zfill(2)
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de líneas - Rentas
                fig_tendencia_rentas = create_line_chart(
                    df_filtrado,
                    x='periodo',
                    y='total_rentas',
                    color='nombre_categoria',
                    title='📦 Evolución de Rentas por Categoría'
                )
                st.plotly_chart(fig_tendencia_rentas, use_container_width=True)
            
            with col2:
                # Gráfico de líneas - Revenue
                fig_tendencia_revenue = create_line_chart(
                    df_filtrado,
                    x='periodo',
                    y='revenue',
                    color='nombre_categoria',
                    title='💰 Evolución de Revenue por Categoría'
                )
                st.plotly_chart(fig_tendencia_revenue, use_container_width=True)
            
            # Análisis de estacionalidad
            st.markdown("#### 📊 Análisis por Mes")
            
            df_por_mes = df_filtrado.groupby('mes_nombre').agg({
                'total_rentas': 'sum',
                'revenue': 'sum'
            }).reset_index()
            
            # Ordenar por meses
            meses_orden = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                          'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
            df_por_mes['mes_nombre'] = pd.Categorical(df_por_mes['mes_nombre'], categories=meses_orden, ordered=True)
            df_por_mes = df_por_mes.sort_values('mes_nombre')
            
            fig_estacionalidad = create_bar_chart(
                df_por_mes,
                x='mes_nombre',
                y='total_rentas',
                color='revenue',
                title='Estacionalidad - Rentas por Mes (categorías seleccionadas)'
            )
            st.plotly_chart(fig_estacionalidad, use_container_width=True)
        else:
            st.warning("⚠️ Selecciona al menos una categoría para visualizar tendencias")

# TAB 3: Top Películas
with tab3:
    st.markdown("### 🎬 Top Películas por Categoría")
    
    if not df_top_films_cat.empty:
        # Selector de categoría
        categorias_lista = df_top_films_cat['nombre_categoria'].unique().tolist()
        categoria_seleccionada = st.selectbox(
            "Seleccionar categoría:",
            options=['Todas'] + sorted(categorias_lista)
        )
        
        if categoria_seleccionada == 'Todas':
            df_filtrado_films = df_top_films_cat.head(20)
            titulo_grafico = "Top 20 Películas (Todas las Categorías)"
        else:
            df_filtrado_films = df_top_films_cat[
                df_top_films_cat['nombre_categoria'] == categoria_seleccionada
            ].head(10)
            titulo_grafico = f"Top 10 Películas - {categoria_seleccionada}"
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            if not df_filtrado_films.empty:
                # Gráfico de barras horizontales
                fig_top_films = create_bar_chart(
                    df_filtrado_films.sort_values('total_rentas', ascending=True),
                    x='total_rentas',
                    y='titulo',
                    color='ingresos_totales',
                    title=titulo_grafico,
                    orientation='h'
                )
                st.plotly_chart(fig_top_films, use_container_width=True)
                
                # Tabla detallada
                st.markdown("#### 📋 Detalle de Películas")
                df_films_display = df_filtrado_films.copy()
                df_films_display['ingresos_totales'] = df_films_display['ingresos_totales'].apply(lambda x: f"${x:,.2f}")
                df_films_display['precio_promedio'] = df_films_display['precio_promedio'].apply(lambda x: f"${x:.2f}")
                
                st.dataframe(
                    df_films_display[['titulo', 'nombre_categoria', 'total_rentas', 'ingresos_totales', 'precio_promedio']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("No hay datos disponibles para la categoría seleccionada")
        
        with col2:
            st.markdown("#### 📊 Estadísticas")
            
            if not df_filtrado_films.empty:
                total_rentas_cat = df_filtrado_films['total_rentas'].sum()
                total_revenue_cat = df_filtrado_films['ingresos_totales'].sum()
                num_peliculas = len(df_filtrado_films)
                precio_prom = df_filtrado_films['precio_promedio'].mean()
                
                st.metric("🎬 Películas", num_peliculas)
                st.metric("📦 Total Rentas", f"{int(total_rentas_cat):,}")
                st.metric("💰 Revenue", f"${total_revenue_cat:,.2f}")
                st.metric("🎟️ Precio Prom.", f"${precio_prom:.2f}")
                
                st.markdown("---")
                
                # Película más popular
                pelicula_top = df_filtrado_films.iloc[0]
                st.info(f"""
                **🏆 Más Popular:**
                
                {pelicula_top['titulo']}
                
                **Rentas:** {int(pelicula_top['total_rentas']):,}
                
                **Revenue:** ${pelicula_top['ingresos_totales']:,.2f}
                """)

# TAB 4: Detalle
with tab4:
    st.markdown("### 📋 Datos Detallados")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if not df_performance.empty:
            st.markdown("#### 📊 Performance Completo por Categoría")
            
            # Formatear datos
            df_perf_display = df_performance.copy()
            df_perf_display['ingresos_totales'] = df_perf_display['ingresos_totales'].apply(lambda x: f"${x:,.2f}")
            df_perf_display['precio_promedio'] = df_perf_display['precio_promedio'].apply(lambda x: f"${x:.2f}")
            df_perf_display['dias_renta_promedio'] = df_perf_display['dias_renta_promedio'].apply(lambda x: f"{x:.1f}")
            
            st.dataframe(
                df_perf_display,
                use_container_width=True,
                hide_index=True,
                height=500
            )
            
            # Botones de descarga
            st.markdown("#### 📥 Exportar Datos")
            
            col_a, col_b, col_c = st.columns(3)
            
            with col_a:
                csv_performance = df_performance.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Performance",
                    data=csv_performance,
                    file_name="categorias_performance.csv",
                    mime="text/csv"
                )
            
            with col_b:
                if not df_tendencia.empty:
                    csv_tendencia = df_tendencia.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Tendencias",
                        data=csv_tendencia,
                        file_name="categorias_tendencia.csv",
                        mime="text/csv"
                    )
            
            with col_c:
                if not df_top_films_cat.empty:
                    csv_films = df_top_films_cat.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Top Películas",
                        data=csv_films,
                        file_name="top_peliculas_categoria.csv",
                        mime="text/csv"
                    )
    
    with col2:
        st.markdown("#### 📈 Resumen Ejecutivo")
        
        if not df_performance.empty:
            # Mejor categoría por revenue
            mejor_cat = df_performance.nlargest(1, 'ingresos_totales').iloc[0]
            highlight_metric(
                "🏆 Top Categoría",
                mejor_cat['nombre_categoria'],
                f"${mejor_cat['ingresos_totales']:,.2f}",
                color="#4CAF50"
            )
            
            # Categoría con más películas
            mas_peliculas = df_performance.nlargest(1, 'total_peliculas').iloc[0]
            highlight_metric(
                "🎬 Más Películas",
                mas_peliculas['nombre_categoria'],
                f"{int(mas_peliculas['total_peliculas'])} películas",
                color="#2196F3"
            )
            
            # Promedio general
            promedio_revenue = df_performance['ingresos_totales'].mean()
            highlight_metric(
                "📊 Revenue Promedio",
                f"${promedio_revenue:,.2f}",
                "Por categoría",
                color="#FF9800"
            )

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888;'>
        <p>📂 Análisis de Categorías | Sakila Data Warehouse</p>
    </div>
""", unsafe_allow_html=True)